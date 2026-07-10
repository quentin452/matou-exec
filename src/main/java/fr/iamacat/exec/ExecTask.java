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
    private volatile R result;
    private volatile boolean done;
    private volatile boolean cancelled;

    ExecTask(Job<S, R> job, S snapshot) {
        this.job = job;
        this.snapshot = snapshot;
    }

    /** Run the pure computation (a cancelled task is skipped but still marked done). */
    void compute() {
        if (!cancelled) {
            result = job.compute(snapshot);
        }
        done = true;
    }

    /** On the tick thread: apply unless the task was cancelled or its context is gone. */
    void applyIfValid() {
        if (!cancelled && job.isValid()) {
            job.apply(result);
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
