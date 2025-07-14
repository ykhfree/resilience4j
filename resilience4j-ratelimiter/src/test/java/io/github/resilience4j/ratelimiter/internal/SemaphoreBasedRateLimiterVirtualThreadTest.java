package io.github.resilience4j.ratelimiter.internal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.lang.reflect.Field;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import io.github.resilience4j.ratelimiter.RateLimiterConfig;

/**
 * Verifies that {@link SemaphoreBasedRateLimiter} executes scheduled refresh tasks
 * on a virtual thread when configured to do so via system property.
 */
public class SemaphoreBasedRateLimiterVirtualThreadTest {

    private static final String SYS_PROP_KEY = "resilience4j.thread.type";

    @After
    public void cleanup() {
        System.clearProperty(SYS_PROP_KEY);
    }

    private static ScheduledExecutorService extractScheduler(SemaphoreBasedRateLimiter limiter)
        throws Exception {
        Field f = SemaphoreBasedRateLimiter.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        return (ScheduledExecutorService) f.get(limiter);
    }

    @Test
    public void limiterUsesVirtualThreadSchedulerWhenConfigured() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        SemaphoreBasedRateLimiter limiter =
            new SemaphoreBasedRateLimiter("vrt", RateLimiterConfig.ofDefaults(), (java.util.concurrent.ScheduledExecutorService) null);

        try {
            ScheduledExecutorService scheduler = extractScheduler(limiter);

            Future<Boolean> isVirtual =
                scheduler.submit(() -> Thread.currentThread().isVirtual());

            assertTrue("Scheduler inside rate limiter should use virtual threads",
                isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void limiterUsesPlatformThreadSchedulerByDefault() throws Exception {
        SemaphoreBasedRateLimiter limiter =
            new SemaphoreBasedRateLimiter("default", RateLimiterConfig.ofDefaults(), (java.util.concurrent.ScheduledExecutorService) null);

        try {
            ScheduledExecutorService scheduler = extractScheduler(limiter);

            Future<Boolean> isVirtual =
                scheduler.submit(() -> Thread.currentThread().isVirtual());

            assertFalse("Scheduler inside rate limiter should default to platform threads",
                isVirtual.get(1, TimeUnit.SECONDS));
        } finally {
            limiter.shutdown();
        }
    }
}
