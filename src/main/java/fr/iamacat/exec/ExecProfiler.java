package fr.iamacat.exec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Opt-in compute-FLOOD profiler for the execution seam (hub doc 28 / devtools) — the diagnostic that surfaces WHERE the
 * engine is doing floods of compute we could reduce with a better algorithm, beyond what {@link ExecStats} (raw
 * submitted/computed/applied + total nanos) can show. Per job-type it tracks, when enabled and at zero cost when not:
 * <ul>
 * <li><b>submission burst rate</b> — submits per WINDOW (the caller closes a window via {@link #rollWindow()}; the seam
 * has no notion of a tick, so the profiler never invents a timer), with the max and most-recent per-window counts, so a
 * type flooding N/window is visible;</li>
 * <li><b>redundancy</b> — submits made while an {@code equals}-equal {@link Job#dedupKey()} was already in flight
 * (submitted but not yet drained) = work recomputed before the first copy even finished; plus a coarse
 * {@code backlog} (submitted-minus-retired) proxy that works even for job types with no dedup key;</li>
 * <li><b>timing tail</b> — the max single {@code compute} nanos and a count of "slow" computes (over
 * {@link #setSlowThresholdNanos(long)}), the p~max hint that one type has an algorithmic tail;</li>
 * <li><b>hotspot attribution</b> — a throttled, capped stack-trace sample (one per type, every Nth submit) of the
 * caller doing the flooding — the "who".</li>
 * </ul>
 *
 * <p>
 * OFF by default: the initial value comes from the {@code -Dmatoulib.exec.profiling} flag (read once via
 * {@link Boolean#getBoolean}, mirroring {@link MatouExec#PARALLEL_FLAG} — this module is shaded and reads {@code -D}
 * System properties, NOT {@code matoulib.properties} files), and can be driven at runtime by matoulib-core through
 * {@link #setEnabled(boolean)}. Every hook is guarded by a single volatile-boolean read before any work, so a disabled
 * profiler allocates nothing and does not touch a map. Thread-safe (compute runs on worker threads): all state is a
 * {@link ConcurrentHashMap} of atomics, like {@link ExecStats}. {@link #reset()} clears the window for a clean A/B;
 * {@link #snapshot()} returns an insertion-ordered per-type report a future {@code /execprofile} endpoint can JSON-ify.
 */
public final class ExecProfiler {

    /** Fallback {@code -D} toggle (initial default only; matoulib-core drives the live state via {@link #setEnabled}). */
    public static final String PROFILING_FLAG = "matoulib.exec.profiling";

    /** Default "slow compute" threshold: 2 ms — a compute over this counts toward the timing tail. */
    public static final long DEFAULT_SLOW_THRESHOLD_NANOS = 2_000_000L;

    /** Default hotspot sampling period: capture a caller stack once every 256 submits per type. */
    public static final int DEFAULT_SAMPLE_EVERY_N = 256;

    /** How many non-internal frames of the sampled caller stack to keep. */
    private static final int SAMPLE_FRAMES = 8;

    private static volatile boolean enabled = Boolean.getBoolean(PROFILING_FLAG);
    private static volatile long slowThresholdNanos = DEFAULT_SLOW_THRESHOLD_NANOS;
    private static volatile int sampleEveryN = DEFAULT_SAMPLE_EVERY_N;

    private static final ConcurrentHashMap<String, TypeProfile> BY_TYPE = new ConcurrentHashMap<>();

    private ExecProfiler() {}

    // --- Toggle -------------------------------------------------------------------------------------------------

    /** Enable/disable profiling at runtime (matoulib-core drives this from {@code matoulib.properties} at wiring). */
    public static void setEnabled(boolean on) {
        enabled = on;
    }

    /** Whether the profiler is currently recording. The hot-path guard: false =&gt; every hook is a no-op. */
    public static boolean isEnabled() {
        return enabled;
    }

    /** A compute over this many nanos counts as a "slow" compute (timing tail). */
    public static void setSlowThresholdNanos(long nanos) {
        slowThresholdNanos = nanos;
    }

    public static long getSlowThresholdNanos() {
        return slowThresholdNanos;
    }

    /** Capture a caller stack sample once every {@code n} submits per type ({@code n &lt;= 0} disables sampling). */
    public static void setSampleEveryN(int n) {
        sampleEveryN = n;
    }

    public static int getSampleEveryN() {
        return sampleEveryN;
    }

    // --- Hooks (called by ExecTask on whichever thread runs the work; each guarded by the enabled read) ----------

    /**
     * Submit hook (on the CALLING/tick thread — the flood source): counts the submit, bumps the current window, records
     * a redundant recompute if an equal {@link Job#dedupKey()} is already in flight, and — throttled — captures the
     * caller stack as the representative hotspot for the type.
     */
    static void submit(String type, Job<?, ?> job) {
        if (!enabled) {
            return;
        }
        TypeProfile tp = of(type);
        tp.submitted.incrementAndGet();
        tp.currentWindow.incrementAndGet();

        Object key = job.dedupKey();
        if (key != null) {
            // merge returns the NEW count; > 1 means an equal key was already in flight => redundant recompute.
            int now = tp.inFlightKeys.merge(key, 1, Integer::sum);
            if (now > 1) {
                tp.redundant.incrementAndGet();
            }
        }

        int every = sampleEveryN;
        if (every > 0) {
            long n = tp.sampleCounter.incrementAndGet();
            if (n % every == 0L) {
                tp.sampledCaller = captureCaller();
            }
        }
    }

    /** Compute hook (may run on a worker thread): tracks the timing tail — max single-compute nanos + slow count. */
    static void compute(String type, long nanos) {
        if (!enabled) {
            return;
        }
        TypeProfile tp = of(type);
        updateMax(tp.maxComputeNanos, nanos);
        if (nanos > slowThresholdNanos) {
            tp.slowComputes.incrementAndGet();
        }
    }

    /**
     * Retire hook (on the tick thread at drain, called once per submitted task whether or not it applied): releases the
     * in-flight dedup key and advances the retired count that feeds the backlog proxy.
     */
    static void retire(String type, Job<?, ?> job) {
        if (!enabled) {
            return;
        }
        TypeProfile tp = of(type);
        tp.retired.incrementAndGet();
        Object key = job.dedupKey();
        if (key != null) {
            tp.inFlightKeys.computeIfPresent(key, (k, v) -> v <= 1 ? null : v - 1);
        }
    }

    // --- Window -----------------------------------------------------------------------------------------------

    /**
     * Close the current profiling window: for every type, snapshot its current-window submit count into
     * {@code recentWindow}, fold it into {@code maxWindow}, and reset the current counter to 0. A future devtools/tick
     * hook calls this once per tick (or per any deterministic boundary) so a type flooding N submits/window is visible.
     * Deterministic and MC-free — the profiler never invents a wall-clock timer. No-op when disabled.
     */
    public static void rollWindow() {
        if (!enabled) {
            return;
        }
        for (TypeProfile tp : BY_TYPE.values()) {
            long c = tp.currentWindow.getAndSet(0L);
            tp.recentWindow.set(c);
            updateMax(tp.maxWindow, c);
        }
    }

    /** Alias for {@link #rollWindow()} — close the current window. */
    public static void markWindow() {
        rollWindow();
    }

    // --- Snapshot / reset -------------------------------------------------------------------------------------

    /** Clear all profiling state (start a fresh measurement window for a clean A/B). */
    public static void reset() {
        BY_TYPE.clear();
    }

    /**
     * Insertion-ordered per-type report a future {@code /execprofile} endpoint can JSON-ify (mirrors the
     * {@link ExecStats#snapshot()} style — a near-instant sample of independent atomic reads, not a locked
     * transaction).
     */
    public static Map<String, ProfileRow> snapshot() {
        Map<String, ProfileRow> out = new LinkedHashMap<>();
        for (Map.Entry<String, TypeProfile> e : BY_TYPE.entrySet()) {
            TypeProfile tp = e.getValue();
            long submitted = tp.submitted.get();
            long retired = tp.retired.get();
            out.put(
                e.getKey(),
                new ProfileRow(
                    submitted,
                    tp.maxWindow.get(),
                    tp.recentWindow.get(),
                    tp.currentWindow.get(),
                    tp.redundant.get(),
                    Math.max(0L, submitted - retired),
                    tp.maxComputeNanos.get(),
                    tp.slowComputes.get(),
                    tp.sampledCaller));
        }
        return out;
    }

    // --- Internals --------------------------------------------------------------------------------------------

    private static TypeProfile of(String type) {
        return BY_TYPE.computeIfAbsent(type, k -> new TypeProfile());
    }

    private static void updateMax(AtomicLong holder, long v) {
        long cur;
        while (v > (cur = holder.get())) {
            if (holder.compareAndSet(cur, v)) {
                return;
            }
        }
    }

    /** Top {@link #SAMPLE_FRAMES} frames of the current thread, skipping this profiler's own frames. */
    private static String[] captureCaller() {
        StackTraceElement[] st = Thread.currentThread()
            .getStackTrace();
        String[] frames = new String[SAMPLE_FRAMES];
        int n = 0;
        for (StackTraceElement f : st) {
            String cn = f.getClassName();
            // Skip Thread.getStackTrace + our own seam frames so the top frame is the actual submitting caller.
            if (cn.equals("java.lang.Thread") || cn.startsWith("fr.iamacat.exec.")) {
                continue;
            }
            frames[n++] = f.toString();
            if (n == SAMPLE_FRAMES) {
                break;
            }
        }
        if (n == frames.length) {
            return frames;
        }
        String[] trimmed = new String[n];
        System.arraycopy(frames, 0, trimmed, 0, n);
        return trimmed;
    }

    /** Per-job-type flood signals (thread-safe atomics, mirrors {@link ExecStats.Counters}). */
    static final class TypeProfile {

        final AtomicLong submitted = new AtomicLong();
        final AtomicLong retired = new AtomicLong();
        final AtomicLong currentWindow = new AtomicLong();
        final AtomicLong recentWindow = new AtomicLong();
        final AtomicLong maxWindow = new AtomicLong();
        final AtomicLong redundant = new AtomicLong();
        final AtomicLong maxComputeNanos = new AtomicLong();
        final AtomicLong slowComputes = new AtomicLong();
        final AtomicLong sampleCounter = new AtomicLong();
        /** In-flight dedup-key multiset (key -&gt; live count); an equal key present at submit = a redundant recompute. */
        final ConcurrentHashMap<Object, Integer> inFlightKeys = new ConcurrentHashMap<>();
        /** One representative caller stack per type (throttled sample); {@code null} until first sampled. */
        volatile String[] sampledCaller;
    }

    /**
     * Immutable per-type snapshot row (POJO with public final fields so a devtools endpoint can JSON-ify it directly).
     */
    public static final class ProfileRow {

        /** Total submits recorded for this type in the current window since {@link #reset()}. */
        public final long submitted;
        /** Highest single-window submit count seen (peak flood). */
        public final long maxWindowSubmits;
        /** Submit count of the most recently closed window. */
        public final long recentWindowSubmits;
        /** Submits in the window currently open (not yet rolled). */
        public final long currentWindowSubmits;
        /** Submits made while an equal {@link Job#dedupKey()} was already in flight (0 if the type has no key). */
        public final long redundant;
        /** Submitted-minus-retired backlog — the coarse redundancy/backpressure proxy (works without a dedup key). */
        public final long backlog;
        /** Longest single {@code compute} observed (ns) — the timing tail. */
        public final long maxComputeNanos;
        /** Count of computes over the slow threshold. */
        public final long slowComputes;
        /** Representative caller stack (top frames) for this type, or {@code null} if none sampled. */
        public final String[] sampledCaller;

        ProfileRow(long submitted, long maxWindowSubmits, long recentWindowSubmits, long currentWindowSubmits,
            long redundant, long backlog, long maxComputeNanos, long slowComputes, String[] sampledCaller) {
            this.submitted = submitted;
            this.maxWindowSubmits = maxWindowSubmits;
            this.recentWindowSubmits = recentWindowSubmits;
            this.currentWindowSubmits = currentWindowSubmits;
            this.redundant = redundant;
            this.backlog = backlog;
            this.maxComputeNanos = maxComputeNanos;
            this.slowComputes = slowComputes;
            this.sampledCaller = sampledCaller;
        }
    }
}
