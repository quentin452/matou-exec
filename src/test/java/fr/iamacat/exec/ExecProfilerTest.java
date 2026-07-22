package fr.iamacat.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Gate for {@link ExecProfiler} — the opt-in compute-flood diagnostic. Verifies the zero-cost OFF path, the flood
 * signals (submission-burst windows, redundancy via dedup key, backlog proxy, timing tail, hotspot sampling), the
 * snapshot shape, and reset. Pure JVM, no Minecraft. Mirrors {@link ExecutionBackendTest} style.
 */
public class ExecProfilerTest {

    @Before
    public void setUp() {
        ExecProfiler.reset();
        ExecProfiler.setSlowThresholdNanos(ExecProfiler.DEFAULT_SLOW_THRESHOLD_NANOS);
        ExecProfiler.setSampleEveryN(ExecProfiler.DEFAULT_SAMPLE_EVERY_N);
        ExecProfiler.setEnabled(false);
    }

    @After
    public void tearDown() {
        ExecProfiler.setEnabled(false);
        ExecProfiler.reset();
    }

    /** A named job (stable type key "SquareJob") with an optional dedup key + tunable compute cost. */
    static final class SquareJob implements Job<Integer, Integer> {

        private final List<Integer> sink;
        private final Object key;
        private final long busyNanos;

        SquareJob(List<Integer> sink) {
            this(sink, null, 0L);
        }

        SquareJob(List<Integer> sink, Object key, long busyNanos) {
            this.sink = sink;
            this.key = key;
            this.busyNanos = busyNanos;
        }

        @Override
        public Integer compute(Integer x) {
            if (busyNanos > 0) {
                long end = System.nanoTime() + busyNanos;
                while (System.nanoTime() < end) {
                    // deterministic spin to exercise the timing tail
                }
            }
            return x * x;
        }

        @Override
        public void apply(Integer r) {
            sink.add(r);
        }

        @Override
        public Object dedupKey() {
            return key;
        }
    }

    private static List<Integer> newSink() {
        return Collections.synchronizedList(new ArrayList<Integer>());
    }

    @Test
    public void disabledRecordsNothing() {
        // profiler is off (setUp) — a full submit/drain cycle must leave the snapshot empty (zero-cost, no map touch).
        SerialBackend backend = new SerialBackend();
        for (int i = 0; i < 50; i++) {
            backend.submit(new SquareJob(newSink()), i);
        }
        backend.drainAndApply();
        backend.shutdown();
        assertTrue("disabled profiler must record nothing", ExecProfiler.snapshot()
            .isEmpty());
    }

    @Test
    public void windowBurstCounts() {
        ExecProfiler.setEnabled(true);
        SerialBackend backend = new SerialBackend();
        // Window 1: 10 submits.
        List<Integer> sink = newSink();
        for (int i = 0; i < 10; i++) {
            backend.submit(new SquareJob(sink), i);
        }
        ExecProfiler.rollWindow();
        // Window 2: 3 submits (leaves the window open — current should read 3).
        for (int i = 0; i < 3; i++) {
            backend.submit(new SquareJob(sink), i);
        }
        backend.drainAndApply();
        backend.shutdown();

        ExecProfiler.ProfileRow row = ExecProfiler.snapshot()
            .get("SquareJob");
        assertNotNull(row);
        assertEquals("total submits across both windows", 13L, row.submitted);
        assertEquals("peak window = first window's 10", 10L, row.maxWindowSubmits);
        assertEquals("recent = last CLOSED window (10)", 10L, row.recentWindowSubmits);
        assertEquals("current open window has 3", 3L, row.currentWindowSubmits);
    }

    @Test
    public void redundancyViaDedupKey() {
        ExecProfiler.setEnabled(true);
        // Use the worker pool with 1 thread + tiny cap so keyed submits pile up in flight before any drain: submitting
        // the SAME key repeatedly while copies are in flight is a redundant recompute.
        List<Integer> sink = newSink();
        WorkerPoolBackend backend = new WorkerPoolBackend(1, 1000);
        List<Handle<Integer>> handles = new ArrayList<>();
        Object sharedKey = "chunk:0,0";
        for (int i = 0; i < 20; i++) {
            handles.add(backend.submit(new SquareJob(sink, sharedKey, 200_000L), i));
        }
        awaitDone(handles);
        backend.drainAndApply();
        backend.shutdown();

        ExecProfiler.ProfileRow row = ExecProfiler.snapshot()
            .get("SquareJob");
        assertNotNull(row);
        assertTrue("some of the 20 same-key submits overlapped in flight => redundant recomputes", row.redundant > 0L);
        assertEquals("all retired => backlog drained to 0", 0L, row.backlog);
    }

    @Test
    public void backlogProxyWithoutDedupKey() {
        ExecProfiler.setEnabled(true);
        // No dedup key -> redundancy falls back to the backlog proxy. Submit into serial (computes inline) but DON'T
        // drain: submitted-minus-retired backlog must reflect the un-applied jobs.
        SerialBackend backend = new SerialBackend();
        List<Integer> sink = newSink();
        for (int i = 0; i < 7; i++) {
            backend.submit(new SquareJob(sink), i);
        }
        ExecProfiler.ProfileRow before = ExecProfiler.snapshot()
            .get("SquareJob");
        assertEquals("no dedup key => redundant stays 0", 0L, before.redundant);
        assertEquals("7 submitted, none drained => backlog 7", 7L, before.backlog);

        backend.drainAndApply();
        backend.shutdown();
        ExecProfiler.ProfileRow after = ExecProfiler.snapshot()
            .get("SquareJob");
        assertEquals("all drained => backlog 0", 0L, after.backlog);
    }

    @Test
    public void timingTailMaxAndSlowCount() {
        ExecProfiler.setEnabled(true);
        ExecProfiler.setSlowThresholdNanos(500_000L); // 0.5 ms
        SerialBackend backend = new SerialBackend();
        List<Integer> sink = newSink();
        // Three deliberately-slow computes (~2 ms each) over the 0.5 ms threshold.
        for (int i = 0; i < 3; i++) {
            backend.submit(new SquareJob(sink, null, 2_000_000L), i);
        }
        backend.drainAndApply();
        backend.shutdown();

        ExecProfiler.ProfileRow row = ExecProfiler.snapshot()
            .get("SquareJob");
        assertNotNull(row);
        assertTrue("max compute nanos reflects the slow work", row.maxComputeNanos >= 1_000_000L);
        assertEquals("all three computes were over the threshold", 3L, row.slowComputes);
    }

    @Test
    public void hotspotSamplingCaptured() {
        ExecProfiler.setEnabled(true);
        ExecProfiler.setSampleEveryN(4); // sample every 4th submit
        SerialBackend backend = new SerialBackend();
        List<Integer> sink = newSink();
        for (int i = 0; i < 8; i++) {
            backend.submit(new SquareJob(sink), i);
        }
        backend.drainAndApply();
        backend.shutdown();

        ExecProfiler.ProfileRow row = ExecProfiler.snapshot()
            .get("SquareJob");
        assertNotNull(row);
        assertNotNull("a caller stack must have been sampled", row.sampledCaller);
        assertTrue("sample has at least one frame", row.sampledCaller.length > 0);
        // The seam's own frames are filtered out, so the top frame is a non-exec caller (this test).
        assertTrue(
            "top sampled frame must not be an internal exec frame",
            !row.sampledCaller[0].contains("fr.iamacat.exec."));
    }

    @Test
    public void samplingDisabledWhenPeriodNonPositive() {
        ExecProfiler.setEnabled(true);
        ExecProfiler.setSampleEveryN(0); // disable sampling
        SerialBackend backend = new SerialBackend();
        List<Integer> sink = newSink();
        for (int i = 0; i < 10; i++) {
            backend.submit(new SquareJob(sink), i);
        }
        backend.drainAndApply();
        backend.shutdown();

        ExecProfiler.ProfileRow row = ExecProfiler.snapshot()
            .get("SquareJob");
        assertNotNull(row);
        assertNull("sampling off => no caller captured", row.sampledCaller);
    }

    @Test
    public void snapshotShapeAndReset() {
        ExecProfiler.setEnabled(true);
        SerialBackend backend = new SerialBackend();
        List<Integer> sink = newSink();
        backend.submit(new SquareJob(sink), 1);
        backend.drainAndApply();
        backend.shutdown();

        Map<String, ExecProfiler.ProfileRow> snap = ExecProfiler.snapshot();
        assertEquals("one job type recorded", 1, snap.size());
        assertTrue("keyed by simple job-type name", snap.containsKey("SquareJob"));

        ExecProfiler.reset();
        assertTrue("reset clears all state", ExecProfiler.snapshot()
            .isEmpty());
    }

    private static void awaitDone(List<? extends Handle<?>> handles) {
        long deadline = System.nanoTime() + 10_000_000_000L; // 10s safety
        for (Handle<?> h : handles) {
            while (!h.isDone()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("job did not complete within 10s");
                }
                Thread.yield();
            }
        }
    }
}
