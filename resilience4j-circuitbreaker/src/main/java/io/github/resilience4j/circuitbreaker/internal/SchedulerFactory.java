package io.github.resilience4j.circuitbreaker.internal;

import io.github.resilience4j.core.ExecutorServiceFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Singleton factory for a {@link ScheduledExecutorService} used by the circuit-breaker
 * module. <p>
 *
 * The chosen executor type (platform vs virtual threads) depends on the current value
 * of {@code resilience4j.thread.type}.  Because tests or applications may change this
 * property at runtime, the factory detects configuration changes and transparently
 * recreates the scheduler so that callers always receive an executor that matches the
 * *current* setting. <p>
 *
 * A {@link #reset()} method is provided mainly for tests to force recreation.
 */
public final class SchedulerFactory {

    private static final class Holder {
        private static final SchedulerFactory INSTANCE = new SchedulerFactory();
    }

    /**
     * Returns the singleton factory instance.
     */
    public static SchedulerFactory getInstance() {
        return Holder.INSTANCE;
    }

    /* ──────────────────────────────
     *  Instance state
     * ────────────────────────────── */

    /** cached scheduler (may be {@code null} until first access) */
    private volatile ScheduledExecutorService scheduler;
    /** remembers whether the cached scheduler was created for virtual threads */
    private volatile boolean virtual;
    /** lock to protect the scheduler state */
    private final ReentrantLock lock = new ReentrantLock();

    private SchedulerFactory() { }

    /**
     * Returns a {@link ScheduledExecutorService} matching the current Resilience4j
     * thread-type configuration.  If the configuration changed since the last call,
     * the previous scheduler is shut down and a new one is created.
     */
    public ScheduledExecutorService getScheduler() {
        lock.lock();
        try {
            boolean desiredVirtual = ExecutorServiceFactory.useVirtualThreads();

            // (Re)create when first call or configuration change
            if (scheduler == null || desiredVirtual != virtual) {
                if (scheduler != null) {
                    scheduler.shutdownNow();
                }
                scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor(
                    "CircuitBreakerAutoTransitionThread");
                virtual = desiredVirtual;
            }
            return scheduler;
        } finally {
            lock.unlock();
        }
    }

    /**
     * For test-code: shut down and forget the current scheduler so that the next
     * {@link #getScheduler()} call creates a fresh executor according to the then
     * active configuration.
     */
    public void reset() {
        lock.lock();
        try {
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }
        } finally {
            lock.unlock();
        }
    }
}
