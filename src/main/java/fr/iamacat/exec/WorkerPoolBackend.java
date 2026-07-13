package fr.iamacat.exec;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parallel {@link ExecutionBackend} (hub doc 28): {@code compute} runs on a bounded worker pool, finished results land
 * on a concurrent queue, and {@link #drainAndApply(long)} applies them on the tick thread. Because {@code compute} is
 * pure + deterministic ({@link Job}) the outcome is identical to {@link SerialBackend} — that equivalence is the
 * transparency guarantee, asserted by the {@code serial == parallel} test.
 *
 * <p>
 * <b>Backpressure</b>: once {@code maxInFlight} jobs are outstanding, further submits degrade to inline execution
 * (like the serial backend) instead of growing an unbounded queue — the system stays responsive under a submit burst.
 */
public final class WorkerPoolBackend implements ExecutionBackend {

    private final ExecutorService pool;
    private final Queue<ExecTask<?, ?>> ready = new ConcurrentLinkedQueue<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final int maxInFlight;
    private final int workers;

    /** Default: (cores - 2) workers (min 1), 256 in-flight cap — mirrors the workflow concurrency heuristic. */
    public WorkerPoolBackend() {
        this(
            Math.max(
                1,
                Runtime.getRuntime()
                    .availableProcessors() - 2),
            256);
    }

    public WorkerPoolBackend(int threads, int maxInFlight) {
        this.maxInFlight = maxInFlight;
        this.workers = Math.max(1, threads);
        this.pool = Executors.newFixedThreadPool(Math.max(1, threads), r -> {
            Thread t = new Thread(r, "matoulib-exec");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public <S, R> Handle<R> submit(Job<S, R> job, S snapshot) {
        ExecTask<S, R> task = new ExecTask<>(job, snapshot);
        if (inFlight.get() >= maxInFlight) {
            // Backpressure: run inline rather than unbounded-queue the work (counts as inline in ExecStats).
            task.compute(true);
            ready.add(task);
            task.markDone(); // done AFTER enqueue — see ExecTask.markDone (BUG-052-B race)
            return task;
        }
        inFlight.incrementAndGet();
        pool.execute(() -> {
            try {
                task.compute(false); // on a worker thread
                ready.add(task);
                // markDone MUST follow ready.add: a waiter that sees isDone() then drains would otherwise poll an
                // empty queue and apply nothing (the intermittent "0 loaded, 0 errors" of BUG-052-B).
                task.markDone();
            } finally {
                inFlight.decrementAndGet();
            }
        });
        return task;
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
        pool.shutdownNow();
        ready.clear();
    }

    @Override
    public String kind() {
        return "worker";
    }

    @Override
    public int workers() {
        return workers;
    }

    @Override
    public int inFlight() {
        return inFlight.get();
    }

    @Override
    public int pendingApply() {
        return ready.size();
    }
}
