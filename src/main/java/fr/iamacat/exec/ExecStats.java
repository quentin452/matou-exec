package fr.iamacat.exec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight, thread-safe counters for the execution seam (hub doc 28 / devtools) — the instrument the parallelism
 * thesis (doc 73 "own the compute") was missing: per job-type it tracks how many jobs were submitted / computed /
 * applied, the total {@code compute} time (the work pushed off the tick) and {@code apply} time (the serial reconcile
 * on the tick), and how many {@code compute}s ran INLINE instead of on a worker (serial backend, or worker-pool
 * backpressure — the signal that parallelism is being lost). Recorded by {@link ExecTask} on whichever thread runs the
 * work; read as a JSON snapshot by the {@code /execstats} devtools endpoint. Off by default at zero cost until the
 * first job runs; {@link #reset()} clears the window for a clean A/B.
 */
public final class ExecStats {

    /** Per-job-type atomic counters. */
    static final class Counters {

        final AtomicLong submitted = new AtomicLong();
        final AtomicLong computed = new AtomicLong();
        final AtomicLong applied = new AtomicLong();
        final AtomicLong computeNanos = new AtomicLong();
        final AtomicLong applyNanos = new AtomicLong();
        final AtomicLong inlineComputed = new AtomicLong();
    }

    private static final ConcurrentHashMap<String, Counters> BY_TYPE = new ConcurrentHashMap<>();

    private ExecStats() {}

    private static Counters of(String type) {
        return BY_TYPE.computeIfAbsent(type, k -> new Counters());
    }

    static void submit(String type) {
        of(type).submitted.incrementAndGet();
    }

    static void compute(String type, long nanos, boolean inline) {
        Counters c = of(type);
        c.computed.incrementAndGet();
        c.computeNanos.addAndGet(nanos);
        if (inline) {
            c.inlineComputed.incrementAndGet();
        }
    }

    static void apply(String type, long nanos) {
        Counters c = of(type);
        c.applied.incrementAndGet();
        c.applyNanos.addAndGet(nanos);
    }

    /** Clear all counters (start a fresh measurement window). */
    public static void reset() {
        BY_TYPE.clear();
    }

    /**
     * Snapshot: job-type &rarr; {@code [submitted, computed, applied, computeNanos, applyNanos, inlineComputed]}.
     * Insertion-ordered copy; safe to read off any thread (each value is an independent atomic read, so the row is a
     * near-instant sample, not a locked transaction).
     */
    public static Map<String, long[]> snapshot() {
        Map<String, long[]> out = new LinkedHashMap<>();
        for (Map.Entry<String, Counters> e : BY_TYPE.entrySet()) {
            Counters c = e.getValue();
            out.put(
                e.getKey(),
                new long[] { c.submitted.get(), c.computed.get(), c.applied.get(), c.computeNanos.get(),
                    c.applyNanos.get(), c.inlineComputed.get() });
        }
        return out;
    }
}
