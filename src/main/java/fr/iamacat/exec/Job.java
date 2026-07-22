package fr.iamacat.exec;

/**
 * A unit of work expressed as DATA, not threads (hub doc 28). The execution seam that lets a matoulib system run
 * serial (inline on the tick) or parallel (worker pool / native) without being rewritten: the system submits a
 * {@code Job} + an immutable snapshot to an {@link ExecutionBackend}, which owns HOW it runs.
 *
 * <p>
 * The contract — the hard part, because MC 1.7.10 is single-thread authoritative on the tick:
 * <ul>
 * <li>{@link #compute(Object)} is <b>PURE</b>: it reads ONLY the immutable {@code snapshot} handed to it — never the
 * live World/Entity/TileEntity — and is <b>deterministic + order-independent</b> (§3). Determinism is what makes
 * {@code serial == parallel} hold; violate it (wall-clock, unseeded RNG, thread-order-dependent reductions) and the
 * backend switch silently changes gameplay.</li>
 * <li>{@link #apply(Object)} runs on the <b>tick thread</b>, mutates state, and <b>reconciles</b> against the live
 * world (which moved since the snapshot) — it is not a blind write.</li>
 * <li>{@link #isValid()} short-circuits {@code apply} when the job's context has vanished (entity dead, chunk
 * unloaded) between submit and drain.</li>
 * </ul>
 *
 * @param <S> the immutable snapshot type consumed by {@code compute}
 * @param <R> the result type produced by {@code compute} and consumed by {@code apply}
 */
public interface Job<S, R> {

    /** Pure, deterministic computation over the immutable snapshot. MUST NOT touch live game state. */
    R compute(S snapshot);

    /** Apply the result on the tick thread, reconciling against the current (possibly changed) world state. */
    void apply(R result);

    /**
     * Is it still worth applying this result? Default yes; override to drop stale work (dead entity, unloaded chunk).
     */
    default boolean isValid() {
        return true;
    }

    /**
     * OPTIONAL identity for redundancy profiling — the "same work" key. Default {@code null} = no identity (a job type
     * that opts out of dedup tracking). When non-null, {@link ExecProfiler} counts a submit as REDUNDANT if an
     * {@code equals}-equal key for the same job type is already in flight (submitted but not yet drained) — i.e. the
     * engine is recomputing work it has not even finished the first time. This is a pure DIAGNOSTIC signal (it never
     * dedups or drops work); override it cheaply (e.g. a chunk coord, an entity id) on hot flooding job types to
     * surface algorithmic waste, leave it {@code null} otherwise. MUST be cheap, immutable, and correctly implement
     * {@code equals}/{@code hashCode}. Read only when {@link ExecProfiler#isEnabled()}.
     */
    default Object dedupKey() {
        return null;
    }
}
