package fr.iamacat.exec;

/**
 * Runs {@link Job}s (hub doc 28). The seam whose implementation is swapped to move a system between serial and
 * parallel execution WITHOUT rewriting the system: {@link SerialBackend} runs {@code compute} inline on the caller's
 * thread; {@link WorkerPoolBackend} runs it on a worker pool; a future native/GPU backend would offload it further.
 * All of them defer {@link Job#apply} to {@link #drainAndApply(long)}, which the game calls once per tick on the tick
 * thread — so the apply timing (and thus the observable behaviour) is identical across backends.
 */
public interface ExecutionBackend {

    /** Submit a job over an immutable snapshot. The backend decides when/where {@code compute} runs. */
    <S, R> Handle<R> submit(Job<S, R> job, S snapshot);

    /**
     * Apply the results of finished jobs on the CALLING thread (the tick thread), skipping cancelled/invalid ones,
     * bounded by {@code budgetNanos} so a burst of completions never spikes a single tick. Left-over results are
     * applied on the next call.
     */
    void drainAndApply(long budgetNanos);

    /** Apply everything ready, unbounded. */
    default void drainAndApply() {
        drainAndApply(Long.MAX_VALUE);
    }

    /** Stop workers and drop any in-flight/pending work. */
    void shutdown();

    // --- Live gauges for the /execstats devtools readout (doc 28). Defaults suit a thread-less backend. ---

    /** Backend kind: {@code "serial"} or {@code "worker"}. */
    default String kind() {
        return "serial";
    }

    /** Worker-thread count (0 = runs inline on the tick thread). */
    default int workers() {
        return 0;
    }

    /** Jobs submitted-but-not-yet-computed on a worker right now (0 for an inline backend). */
    default int inFlight() {
        return 0;
    }

    /** Computed results waiting to be applied on the next {@code drainAndApply} (drain backlog). */
    default int pendingApply() {
        return 0;
    }

    /**
     * Soft in-flight capacity (the point past which the backend is oversubscribed). A <b>fire-and-forget</b> caller —
     * one that submits without block-waiting on the handle, e.g. the client render thread scheduling mesh/light jobs —
     * should gate on {@code inFlight() < capacity()} and SKIP/retry-next-tick rather than pile more work on a saturated
     * backend. Crossing the cap never makes the backend run a job inline on the caller (that was the render-thread
     * freeze, hub QUEUE #2); it only defers to the pool. {@link Integer#MAX_VALUE} = no cap (an inline/serial backend
     * runs on the caller by design, so there is nothing to gate).
     */
    default int capacity() {
        return Integer.MAX_VALUE;
    }
}
