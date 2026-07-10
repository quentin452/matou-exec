package fr.iamacat.exec;

/**
 * Holder for matoulib's shared {@link ExecutionBackend} (hub doc 28). The backend choice is the ONE switch that moves
 * every seam consumer between serial and parallel: {@link SerialBackend} by default (deterministic, safe, zero
 * threads), or {@link WorkerPoolBackend} when {@code -Dmatoulib.exec.parallel=true}. Because the seam guarantees
 * {@code serial == parallel}, flipping the flag changes only WHERE work runs, never the result.
 */
public final class MatouExec {

    /** Opt into the worker-pool backend for all seam consumers. */
    public static final String PARALLEL_FLAG = "matoulib.exec.parallel";

    private static volatile ExecutionBackend backend;

    private MatouExec() {}

    /** The shared backend (lazily created from the flag). */
    public static ExecutionBackend backend() {
        ExecutionBackend b = backend;
        if (b == null) {
            synchronized (MatouExec.class) {
                b = backend;
                if (b == null) {
                    b = Boolean.getBoolean(PARALLEL_FLAG) ? new WorkerPoolBackend() : new SerialBackend();
                    backend = b;
                }
            }
        }
        return b;
    }

    /** Override the shared backend (tests / explicit wiring). Pass {@code null} to reset to the flag default. */
    public static synchronized void set(ExecutionBackend b) {
        if (backend != null && backend != b) {
            backend.shutdown();
        }
        backend = b;
    }
}
