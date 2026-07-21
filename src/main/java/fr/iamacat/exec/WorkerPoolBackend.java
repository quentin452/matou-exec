package fr.iamacat.exec;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parallel {@link ExecutionBackend} (hub doc 28): {@code compute} runs on a bounded worker pool, finished results land
 * on a concurrent queue, and {@link #drainAndApply(long)} applies them on the tick thread. Because {@code compute} is
 * pure + deterministic ({@link Job}) the outcome is identical to {@link SerialBackend} — that equivalence is the
 * transparency guarantee, asserted by the {@code serial == parallel} test.
 *
 * <p>
 * <b>Backpressure = DEFER, never inline.</b> Once {@code maxInFlight} jobs are outstanding the backend is
 * oversubscribed, but an over-cap submit still runs on a WORKER — it is <b>never</b> executed inline on the calling
 * thread. Inlining under backpressure was the original design (mirror the serial backend to avoid an unbounded queue),
 * but when the caller is the client RENDER thread, inlining a heavy job (measured: a 590K-cell light flood during
 * chunk-gen flooding) froze the frame to ~3fps — heavy compute on the render/tick thread is the one thing this seam
 * must never do (hub QUEUE #2, jstack-confirmed 2026-07-21; invisible to CPU profilers = safepoint bias on counted
 * loops). So the cap is now a SOFT gate: it drives {@link #capacity()} and a throttled oversubscription warning, and
 * fire-and-forget callers should check {@code inFlight() < capacity()} and skip/retry rather than pile on. Block-waiting
 * callers (submit one job, spin on {@link Handle#isDone()}) are naturally bounded and simply wait for a worker.
 */
public final class WorkerPoolBackend implements ExecutionBackend {

    private final ExecutorService pool;
    private final Queue<ExecTask<?, ?>> ready = new ConcurrentLinkedQueue<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong deferred = new AtomicLong();
    private volatile long lastWarnNanos;
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
        int now = inFlight.incrementAndGet();
        if (now > maxInFlight) {
            // OVER SOFT CAP: DEFER, do NOT inline. The job still runs on a worker (below), NEVER on the calling
            // thread — inlining a heavy job on the render/tick thread was the ~3fps freeze (hub QUEUE #2). The
            // pool's queue absorbs the overflow; a throttled warning surfaces persistent oversubscription and
            // fire-and-forget callers should gate on inFlight() < capacity() to keep the backlog bounded.
            warnDeferred(now);
        }
        pool.execute(() -> {
            try {
                task.compute(false); // ALWAYS on a worker thread — never inline on the caller, even under backpressure
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

    /** Throttled (first hit, then at most ~1/s) stderr warning that the backend is oversubscribed. */
    private void warnDeferred(int inFlightNow) {
        long n = deferred.incrementAndGet();
        long t = System.nanoTime();
        long last = lastWarnNanos;
        if (n == 1L || t - last > 1_000_000_000L) {
            lastWarnNanos = t;
            System.err.println(
                "[matou-exec] WorkerPoolBackend oversubscribed (" + inFlightNow + " > cap " + maxInFlight
                    + ", deferred=" + n + "): deferring to workers, NOT inlining on the caller.");
        }
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
    public int capacity() {
        return maxInFlight;
    }

    @Override
    public int pendingApply() {
        return ready.size();
    }
}
