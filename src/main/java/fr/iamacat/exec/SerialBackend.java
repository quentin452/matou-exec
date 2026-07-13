package fr.iamacat.exec;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * The reference {@link ExecutionBackend} (hub doc 28): {@code compute} runs INLINE on the caller's thread at submit
 * time, and {@code apply} runs at {@link #drainAndApply(long)}. Deterministic by construction and dependency-free — it
 * is the baseline the {@code serial == parallel} test compares against, the debug/low-end fallback, and the safe home
 * for jobs classified {@code must-run-on-tick}. Single-threaded (used from the tick thread); no synchronisation.
 */
public final class SerialBackend implements ExecutionBackend {

    private final Queue<ExecTask<?, ?>> ready = new ArrayDeque<>();

    @Override
    public <S, R> Handle<R> submit(Job<S, R> job, S snapshot) {
        ExecTask<S, R> task = new ExecTask<>(job, snapshot);
        task.compute(true); // inline on the caller (tick) thread
        ready.add(task);
        task.markDone(); // done after enqueue, mirroring the parallel backend's contract (ExecTask.markDone)
        return task;
    }

    @Override
    public int pendingApply() {
        return ready.size();
    }

    @Override
    public void drainAndApply(long budgetNanos) {
        long start = System.nanoTime();
        ExecTask<?, ?> task;
        while ((task = ready.poll()) != null) {
            task.applyIfValid();
            if (System.nanoTime() - start > budgetNanos) {
                break;
            }
        }
    }

    @Override
    public void shutdown() {
        ready.clear();
    }
}
