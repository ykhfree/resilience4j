package io.github.resilience4j.circuitbreaker.internal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

/**
 * Ensures that the {@link SchedulerFactory} creates a {@link ScheduledExecutorService}
 * backed by virtual threads when the global system property
 * {@code resilience4j.thread.type} is set to {@code virtual}.
 */
public class SchedulerFactoryVirtualThreadTest {

    private static final String SYS_PROP_KEY = "resilience4j.thread.type";

    @After
    public void clearProperty() {
        System.clearProperty(SYS_PROP_KEY);
    }

    @Test
    public void schedulerRunsTasksOnVirtualThreadsWhenConfigured() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        ScheduledExecutorService scheduler = SchedulerFactory.getInstance().getScheduler();

        Future<Boolean> isVirtual = scheduler.submit(() -> Thread.currentThread().isVirtual());

        try {
            assertTrue("Task executed by SchedulerFactory should run on a virtual thread",
                isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void schedulerRunsTasksOnPlatformThreadsByDefault() throws Exception {
        ScheduledExecutorService scheduler = SchedulerFactory.getInstance().getScheduler();

        Future<Boolean> isVirtual = scheduler.submit(() -> Thread.currentThread().isVirtual());

        try {
            assertFalse("Task executed by SchedulerFactory should run on a platform thread by default",
                isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            scheduler.shutdownNow();
        }
    }
}
