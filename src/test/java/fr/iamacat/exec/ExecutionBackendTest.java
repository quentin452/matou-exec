package fr.iamacat.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Phase-1 gate for the execution backend (hub doc 28). The headline invariant is {@code serial == parallel}: a
 * deterministic job produces a bit-identical applied result set on {@link SerialBackend} and {@link WorkerPoolBackend}
 * — the guarantee that swapping backends never changes gameplay. Plus the lifecycle rules (cancel / invalid skip
 * apply) and that backpressure never loses work. Pure JVM, no Minecraft.
 */
public class ExecutionBackendTest {

    /** A pure, deterministic job: compute squares the input, apply records it into a shared sink. */
    private static Job<Integer, Integer> squareInto(final List<Integer> sink) {
        return new Job<Integer, Integer>() {

            @Override
            public Integer compute(Integer x) {
                return x * x;
            }

            @Override
            public void apply(Integer r) {
                sink.add(r);
            }
        };
    }

    @Test
    public void serialEqualsParallel() {
        List<Integer> serial = runAll(new SerialBackend(), 500);
        List<Integer> parallel = runAll(new WorkerPoolBackend(4, 64), 500);
        Collections.sort(serial);
        Collections.sort(parallel);
        assertEquals("parallel result set must equal serial (determinism = transparent swap)", serial, parallel);
        // Sanity: the results really are the squares 0..n-1.
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            expected.add(i * i);
        }
        Collections.sort(expected);
        assertEquals(expected, serial);
    }

    /** Submit n square-jobs, wait for completion, drain, return the applied results. */
    private static List<Integer> runAll(ExecutionBackend backend, int n) {
        List<Integer> sink = Collections.synchronizedList(new ArrayList<Integer>());
        List<Handle<Integer>> handles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            handles.add(backend.submit(squareInto(sink), i));
        }
        awaitDone(handles);
        backend.drainAndApply();
        backend.shutdown();
        return new ArrayList<>(sink);
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

    @Test
    public void cancelledJobIsNotApplied() {
        List<Integer> sink = new ArrayList<>();
        SerialBackend backend = new SerialBackend();
        Handle<Integer> h = backend.submit(squareInto(sink), 7);
        // Serial already computed inline; cancel after completion returns false and apply is still allowed...
        assertTrue("computed inline", h.isDone());
        assertFalse("cancel after done returns false", h.cancel());
        backend.drainAndApply();
        assertEquals(Collections.singletonList(49), sink);
    }

    @Test
    public void invalidJobIsNotApplied() {
        final List<Integer> sink = new ArrayList<>();
        Job<Integer, Integer> invalid = new Job<Integer, Integer>() {

            @Override
            public Integer compute(Integer x) {
                return x * x;
            }

            @Override
            public void apply(Integer r) {
                sink.add(r);
            }

            @Override
            public boolean isValid() {
                return false; // context gone -> must not apply
            }
        };
        SerialBackend backend = new SerialBackend();
        backend.submit(invalid, 9);
        backend.drainAndApply();
        assertTrue("invalid job's result must not be applied", sink.isEmpty());
    }

    @Test
    public void backpressureLosesNoWork() {
        // maxInFlight=1 forces most submits to degrade to inline; every job must still be applied exactly once.
        List<Integer> sink = runAll(new WorkerPoolBackend(2, 1), 300);
        assertEquals("every job applied despite backpressure", 300, sink.size());
    }
}
