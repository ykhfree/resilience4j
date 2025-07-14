package io.github.resilience4j.core;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

/**
 * Verifies that {@link ExecutorServiceFactory} respects the
 * {@code resilience4j.thread.type} system property and actually spawns Java 21
 * virtual threads when configured so.
 */
public class ExecutorServiceFactoryVirtualThreadTest {

    private static final String SYS_PROP_KEY = "resilience4j.thread.type";

    @After
    public void clearProperty() {
        System.clearProperty(SYS_PROP_KEY);
    }

    @Test
    public void shouldUseVirtualThreadsWhenSystemPropertyIsVirtual() {
        System.setProperty(SYS_PROP_KEY, "virtual");
        assertTrue("useVirtualThreads() must return true when system property is set to 'virtual'",
                ExecutorServiceFactory.useVirtualThreads());
    }

    @Test
    public void scheduledExecutorProducesVirtualThreads() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        ScheduledExecutorService executor =
                ExecutorServiceFactory.newSingleThreadScheduledExecutor("vrt-test");

        Future<Boolean> isVirtual = executor.submit(() -> Thread.currentThread().isVirtual());

        try {
            assertTrue("Task should run on a virtual thread when virtual mode is enabled",
                    isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldFallbackToPlatformThreadsWhenNotConfigured() throws Exception {
        assertFalse("useVirtualThreads() must be false when no property is set",
                ExecutorServiceFactory.useVirtualThreads());

        ScheduledExecutorService executor =
                ExecutorServiceFactory.newSingleThreadScheduledExecutor("vrt-test-default");

        Future<Boolean> isVirtual = executor.submit(() -> Thread.currentThread().isVirtual());

        try {
            assertFalse("Task should run on a platform thread by default",
                    isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
