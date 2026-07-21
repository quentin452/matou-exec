package fr.iamacat.exec;

/**
 * Package-private {@link Handle} implementation shared by the backends: it carries a job + its snapshot, runs the pure
 * {@code compute} once, and defers {@code apply} to drain time (skipping cancelled/invalid tasks). {@code compute} may
 * run on a worker thread and {@code applyIfValid} on the tick thread, so the fields crossing that boundary are
 * {@code volatile}.
 */
final class ExecTask<S, R> implements Handle<R> {

    private final Job<S, R> job;
    private final S snapshot;
    private final String type;
    private volatile R result;
    private volatile boolean done;
    private volatile boolean cancelled;

    ExecTask(Job<S, R> job, S snapshot) {
        this.job = job;
        this.snapshot = snapshot;
        this.type = job.getClass()
            .getSimpleName();
        ExecStats.submit(type);
    }

    /**
     * Run the pure computation (a cancelled task is skipped), timing it for {@link ExecStats}. {@code inline} = the
     * backend ran it on the CALLING (tick) thread (the serial backend) rather than a worker — high inline counts mean
     * parallelism is being lost. The worker pool never inlines, even under backpressure (it defers to a worker), so its
     * inline count stays 0.
     */
    void compute(boolean inline) {
        if (!cancelled) {
            long t0 = System.nanoTime();
            result = job.compute(snapshot);
            ExecStats.compute(type, System.nanoTime() - t0, inline);
        }
    }

    /**
     * Mark the task done — its result is computed AND it has been enqueued for drain. A backend MUST call this AFTER
     * adding the task to its ready queue, never inside {@link #compute()}: {@code done} is the signal a waiter polls
     * ({@code while (!isDone())}) before draining, so if it flipped true before the task reached the ready queue, the
     * drain would find an empty queue and silently apply nothing (the WorkerPool submit→compute→add race that made
     * one-shot DSL loads intermittently return "0 loaded, 0 errors" — hub BUG-052-B).
     */
    void markDone() {
        done = true;
    }

    /** On the tick thread: apply unless the task was cancelled or its context is gone (timed for {@link ExecStats}). */
    void applyIfValid() {
        if (!cancelled && job.isValid()) {
            long t0 = System.nanoTime();
            job.apply(result);
            ExecStats.apply(type, System.nanoTime() - t0);
        }
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean cancel() {
        if (done) {
            return false;
        }
        cancelled = true;
        return true;
    }

    @Override
    public boolean isValid() {
        return job.isValid();
    }
}
