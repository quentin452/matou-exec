package fr.iamacat.exec;

/**
 * A submitted {@link Job}'s lifecycle handle (hub doc 28). Lets the submitter observe completion and cancel work whose
 * context died before it could be applied (entity killed / chunk unloaded mid-flight), so a dead job never mutates the
 * world at drain time.
 *
 * @param <R> the job's result type
 */
public interface Handle<R> {

    /** True once {@code compute} has finished (or the job was cancelled). */
    boolean isDone();

    boolean isCancelled();

    /** Request cancellation; returns false if the job already completed. A cancelled job is never applied. */
    boolean cancel();

    /** Delegates to {@link Job#isValid()} — whether the result is still worth applying. */
    boolean isValid();
}
