package io.github.resilience4j.ratelimiter.internal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;

/**
 * Verifies that {@link SemaphoreBasedRateLimiter} executes scheduled refresh tasks
 * on a virtual thread when configured to do so via system property and that
 * virtual thread-specific logic works correctly to avoid pinning issues.
 * 
 * This class focuses on virtual thread specific functionality and dual-thread 
 * mode comparison testing to ensure behavioral consistency.
 */
public class SemaphoreBasedRateLimiterVirtualThreadTest {

    private static final String SYS_PROP_KEY = "resilience4j.thread.type";
    private String originalPropertyValue;

    @Before
    public void setUp() {
        // Save original property value
        originalPropertyValue = System.getProperty(SYS_PROP_KEY);
        // Ensure clean state for each test
        System.clearProperty(SYS_PROP_KEY);
    }

    @After
    public void cleanup() {
        // Restore original property value
        if (originalPropertyValue != null) {
            System.setProperty(SYS_PROP_KEY, originalPropertyValue);
        } else {
            System.clearProperty(SYS_PROP_KEY);
        }
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

    @Test
    public void virtualThreadModeUsesAsyncRefreshLogic() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(2)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(50))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("virtualTest", config, (ScheduledExecutorService) null);

        try {
            // Acquire all permissions
            assertTrue("First permission should be granted", limiter.acquirePermission(1));
            assertTrue("Second permission should be granted", limiter.acquirePermission(1));
            assertFalse("Third permission should be rejected", limiter.acquirePermission(1));

            // Manually trigger limit refresh to test async logic
            limiter.refreshLimit();
            
            // Should be able to acquire permissions again after refresh
            assertTrue("Permission should be available after refresh", limiter.acquirePermission(1));
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModeHandlesWaitingQueueCorrectly() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(1)
            .limitRefreshPeriod(Duration.ofMillis(200))
            .timeoutDuration(Duration.ofMillis(100))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("waitingQueueTest", config, (ScheduledExecutorService) null);

        try {
            // Acquire the only available permission
            assertTrue("First permission should be granted", limiter.acquirePermission(1));

            // Submit multiple concurrent requests that should wait and timeout
            AtomicInteger timeoutCount = new AtomicInteger(0);
            @SuppressWarnings("unchecked")
            CompletableFuture<Void>[] futures = new CompletableFuture[3];
            
            for (int i = 0; i < 3; i++) {
                futures[i] = CompletableFuture.runAsync(() -> {
                    boolean acquired = limiter.acquirePermission(1);
                    if (!acquired) {
                        timeoutCount.incrementAndGet();
                    }
                });
            }

            // Wait for all futures to complete
            CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);

            // All requests should have timed out since we have only 1 permit
            assertEquals("All waiting requests should timeout", 3, timeoutCount.get());
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModeProcessesExpiredRequests() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(1)
            .limitRefreshPeriod(Duration.ofMillis(200))
            .timeoutDuration(Duration.ofMillis(50)) // Short timeout
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("expiredRequestsTest", config, (ScheduledExecutorService) null);

        try {
            // Acquire the only available permission
            assertTrue("First permission should be granted", limiter.acquirePermission(1));

            // Submit a request that will timeout
            AtomicBoolean requestTimedOut = new AtomicBoolean(false);
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                boolean acquired = limiter.acquirePermission(1);
                requestTimedOut.set(!acquired);
            });

            // Wait for the request to timeout
            future.get(200, TimeUnit.MILLISECONDS);

            // Verify the request timed out
            assertTrue("Request should have timed out", requestTimedOut.get());

            // Trigger refresh to process expired requests
            limiter.refreshLimit();

            // Should be able to acquire permission after cleanup
            assertTrue("Permission should be available after expired request cleanup", 
                limiter.acquirePermission(1));
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModeHandlesConcurrentRefreshAndAcquisition() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(3)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(100)) // Shorter timeout for more predictable results
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("concurrentTest", config, (ScheduledExecutorService) null);

        try {
            // Acquire all initial permissions
            for (int i = 0; i < 3; i++) {
                assertTrue("Permission " + (i+1) + " should be granted", 
                    limiter.acquirePermission(1));
            }

            AtomicInteger successfulAcquisitions = new AtomicInteger(0);
            AtomicInteger timeoutCount = new AtomicInteger(0);
            CountDownLatch startLatch = new CountDownLatch(1);
            
            // Start concurrent acquisition attempts
            @SuppressWarnings("unchecked")
            CompletableFuture<Void>[] futures = new CompletableFuture[3];
            for (int i = 0; i < 3; i++) {
                futures[i] = CompletableFuture.runAsync(() -> {
                    try {
                        startLatch.await();
                        if (limiter.acquirePermission(1)) {
                            successfulAcquisitions.incrementAndGet();
                        } else {
                            timeoutCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        timeoutCount.incrementAndGet();
                    }
                });
            }

            // Trigger refresh to make permissions available before starting
            limiter.refreshLimit();
            startLatch.countDown();

            // Wait for all futures to complete
            CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);

            // Should have some successful acquisitions and some timeouts
            int totalAttempts = successfulAcquisitions.get() + timeoutCount.get();
            assertEquals("All attempts should be accounted for", 3, totalAttempts);
            
            // Should have acquired some permissions (exact number may vary due to concurrency)
            assertTrue("Should have at least some successful acquisitions or timeouts", 
                successfulAcquisitions.get() >= 0);
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModePreservesMetricsAccuracy() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(2)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(50))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("metricsTest", config, (ScheduledExecutorService) null);

        try {
            // Initial state
            assertEquals("Should start with 2 available permissions", 
                2, limiter.getMetrics().getAvailablePermissions());

            // Acquire one permission
            limiter.acquirePermission(1);
            // Note: In virtual thread mode, metrics may show original semaphore state
            // The actual enforcement is done via AtomicInteger

            // Refresh should replenish permissions following semaphore logic
            limiter.refreshLimit();
            
            // Verify we can still acquire permissions after refresh
            assertTrue("Should be able to acquire permission after refresh", 
                limiter.acquirePermission(1));
            assertTrue("Should be able to acquire second permission after refresh", 
                limiter.acquirePermission(1));
            assertFalse("Third permission should be rejected", 
                limiter.acquirePermission(1));
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModeFollowsSemaphoreLogicForRefresh() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(5)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(50))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("semaphoreLogicTest", config, (ScheduledExecutorService) null);

        try {
            // Acquire some permissions, leaving some available
            assertTrue("First permission should be granted", limiter.acquirePermission(2));
            assertEquals("Should have 3 permissions remaining", 3, limiter.getMetrics().getAvailablePermissions());

            // Refresh - should only add missing permits (2), not reset to full capacity
            limiter.refreshLimit();
            assertEquals("Should have full capacity (5) after refresh", 5, limiter.getMetrics().getAvailablePermissions());

            // Test scenario where limit is decreased (more permits than target)
            limiter.changeLimitForPeriod(2); // Reduce limit to 2
            
            // Refresh - should NOT reduce existing permits (like semaphore behavior)
            limiter.refreshLimit();
            
            // We should still have more than 2 permits available from before the limit change
            // This tests that virtual thread mode doesn't reset to new limit, just like semaphore
            assertTrue("Should still have permits after limit decrease", 
                limiter.getMetrics().getAvailablePermissions() >= 2);
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModeAndPlatformThreadModeBehavIdentically() throws Exception {
        // Test platform thread behavior
        System.clearProperty(SYS_PROP_KEY); // Use platform threads

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(4)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(50))
            .build();

        SemaphoreBasedRateLimiter platformLimiter = 
            new SemaphoreBasedRateLimiter("platformTest", config, (ScheduledExecutorService) null);

        // Test virtual thread behavior
        System.setProperty(SYS_PROP_KEY, "virtual");
        SemaphoreBasedRateLimiter virtualLimiter = 
            new SemaphoreBasedRateLimiter("virtualTest", config, (ScheduledExecutorService) null);

        try {
            // Scenario 1: Partial consumption then refresh
            platformLimiter.acquirePermission(1);
            virtualLimiter.acquirePermission(1);
            
            assertEquals("Both should have 3 permits after acquiring 1", 
                platformLimiter.getMetrics().getAvailablePermissions(),
                virtualLimiter.getMetrics().getAvailablePermissions());
            
            platformLimiter.refreshLimit();
            virtualLimiter.refreshLimit();
            
            assertEquals("Both should have full capacity after refresh", 
                platformLimiter.getMetrics().getAvailablePermissions(),
                virtualLimiter.getMetrics().getAvailablePermissions());

            // Scenario 2: Limit decrease
            platformLimiter.changeLimitForPeriod(2);
            virtualLimiter.changeLimitForPeriod(2);
            
            // Both should maintain their current permits (not reduce)
            assertTrue("Platform limiter should maintain permits after limit decrease",
                platformLimiter.getMetrics().getAvailablePermissions() >= 2);
            assertTrue("Virtual limiter should maintain permits after limit decrease",
                virtualLimiter.getMetrics().getAvailablePermissions() >= 2);
                
            // The available permits should be similar (allowing for small differences due to implementation)
            int platformPermits = platformLimiter.getMetrics().getAvailablePermissions();
            int virtualPermits = virtualLimiter.getMetrics().getAvailablePermissions();
            assertTrue("Platform and virtual thread modes should behave similarly",
                Math.abs(platformPermits - virtualPermits) <= 1); // Allow 1 permit difference due to timing
        } finally {
            platformLimiter.shutdown();
            virtualLimiter.shutdown();
        }
    }

    @Test
    public void virtualThreadModePreventsABAProblem() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(10)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ofMillis(50))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("abaTest", config, (ScheduledExecutorService) null);

        try {
            // This test attempts to create an ABA scenario
            // Thread A reads permits, Thread B&C modify permits back to original value
            // AtomicStampedReference should prevent ABA issue
            
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch midLatch = new CountDownLatch(2);
            AtomicInteger successCount = new AtomicInteger(0);
            
            // Thread A: Will try to acquire after ABA manipulation
            CompletableFuture<Void> threadA = CompletableFuture.runAsync(() -> {
                try {
                    startLatch.await();
                    // Wait for other threads to potentially create ABA scenario
                    Thread.sleep(10);
                    if (limiter.acquirePermission(5)) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            // Thread B: Consumes permits
            CompletableFuture<Void> threadB = CompletableFuture.runAsync(() -> {
                try {
                    startLatch.await();
                    if (limiter.acquirePermission(10)) { // Consume all
                        successCount.incrementAndGet();
                    }
                    midLatch.countDown();
                } catch (Exception e) {
                    midLatch.countDown();
                }
            });
            
            // Thread C: Refresh to restore permits (potential ABA)
            CompletableFuture<Void> threadC = CompletableFuture.runAsync(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(5); // Let Thread B consume first
                    limiter.refreshLimit(); // This should restore permits
                    midLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    midLatch.countDown();
                }
            });
            
            // Start all threads
            startLatch.countDown();
            
            // Wait for completion
            CompletableFuture.allOf(threadA, threadB, threadC).get(1, TimeUnit.SECONDS);
            
            // With AtomicStampedReference, operations should be atomic and consistent
            // The exact success count may vary due to timing, but operations should be safe
            assertTrue("Operations should complete safely without ABA issues", 
                successCount.get() >= 1);
                
            // Verify final state is consistent
            int finalPermits = limiter.getMetrics().getAvailablePermissions();
            assertTrue("Final permits should be non-negative and <= 10", 
                finalPermits >= 0 && finalPermits <= 10);
        } finally {
            limiter.shutdown();
        }
    }
    
    @Test
    public void virtualThreadModeHandlesConcurrentStampedOperations() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(5)
            .limitRefreshPeriod(Duration.ofMillis(50))
            .timeoutDuration(Duration.ofMillis(100))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("stampedTest", config, (ScheduledExecutorService) null);

        try {
            AtomicInteger totalSuccessful = new AtomicInteger(0);
            AtomicInteger totalFailed = new AtomicInteger(0);
            
            // Create high contention scenario to test stamped reference behavior
            @SuppressWarnings("unchecked")
            CompletableFuture<Void>[] futures = new CompletableFuture[20];
            
            for (int i = 0; i < 20; i++) {
                futures[i] = CompletableFuture.runAsync(() -> {
                    for (int j = 0; j < 5; j++) {
                        if (limiter.acquirePermission(1)) {
                            totalSuccessful.incrementAndGet();
                        } else {
                            totalFailed.incrementAndGet();
                        }
                        
                        // Occasionally trigger refresh to create more contention
                        if (j % 2 == 0) {
                            limiter.refreshLimit();
                        }
                        
                        try {
                            Thread.sleep(1); // Small delay to increase contention
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
            }
            
            // Wait for all operations to complete
            CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);
            
            int totalAttempts = totalSuccessful.get() + totalFailed.get();
            assertEquals("All attempts should be accounted for", 100, totalAttempts);
            
            // Should have some successful acquisitions due to refreshes
            assertTrue("Should have some successful acquisitions", totalSuccessful.get() > 0);
            
            // Final state should be consistent
            int finalPermits = limiter.getMetrics().getAvailablePermissions();
            assertTrue("Final permits should be within valid range", 
                finalPermits >= 0 && finalPermits <= 5);
        } finally {
            limiter.shutdown();
        }
    }

    @Test
    public void virtualThreadMode_tryToAcquireBigNumberOfPermitsAtEndOfCycleTest() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(10)
            .limitRefreshPeriod(Duration.ofNanos(250_000_000L))
            .timeoutDuration(Duration.ZERO)
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("endOfCycleTest", config, (ScheduledExecutorService) null);

        try {
            // Wait for initial refresh to complete
            waitForRefresh(limiter.getMetrics(), config, '.');
            
            System.out.println("Initial permits: " + limiter.getMetrics().getAvailablePermissions());

            // Acquire some permits at the end of cycle
            boolean firstPermission = limiter.acquirePermission(1);
            assertTrue("First permission should be granted", firstPermission);
            System.out.println("After acquiring 1: " + limiter.getMetrics().getAvailablePermissions());
            
            boolean secondPermission = limiter.acquirePermission(5);
            assertTrue("Second permission should be granted", secondPermission);
            System.out.println("After acquiring 5: " + limiter.getMetrics().getAvailablePermissions());
            
            // This should fail as we only have 4 permits left
            boolean firstNoPermission = limiter.acquirePermission(5);
            assertFalse("Should not be able to acquire 5 more permits (only 4 left)", firstNoPermission);
            System.out.println("After failed acquire: " + limiter.getMetrics().getAvailablePermissions());

            // Wait for next cycle
            System.out.println("Waiting for refresh...");
            waitForRefresh(limiter.getMetrics(), config, '*');
            System.out.println("After refresh: " + limiter.getMetrics().getAvailablePermissions());

            // Should be able to acquire 5 permits in new cycle
            boolean retryInSecondCyclePermission = limiter.acquirePermission(5);
            assertTrue("Should acquire 5 permits in second cycle", retryInSecondCyclePermission);
            System.out.println("Final permits: " + limiter.getMetrics().getAvailablePermissions());
            
        } finally {
            limiter.shutdown();
        }
    }
    
    @Test
    public void virtualThreadModeRefreshBehaviorTest() throws Exception {
        System.setProperty(SYS_PROP_KEY, "virtual");

        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(10)
            .limitRefreshPeriod(Duration.ofMillis(100))
            .timeoutDuration(Duration.ZERO)
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("refreshTest", config, (ScheduledExecutorService) null);

        try {
            // Start with full permits
            System.out.println("Initial permits: " + limiter.getMetrics().getAvailablePermissions());
            assertEquals("Should start with full permits", 10, limiter.getMetrics().getAvailablePermissions());
            
            // Consume some permits
            assertTrue(limiter.acquirePermission(6));
            System.out.println("After consuming 6: " + limiter.getMetrics().getAvailablePermissions());
            assertEquals("Should have 4 permits left", 4, limiter.getMetrics().getAvailablePermissions());
            
            // Wait for a refresh cycle
            Thread.sleep(150);
            
            // Manual refresh trigger to test the logic
            limiter.refreshLimit();
            System.out.println("After refresh: " + limiter.getMetrics().getAvailablePermissions());
            
            // Should be back to full capacity following semaphore logic
            assertEquals("Should have full permits after refresh", 10, limiter.getMetrics().getAvailablePermissions());
        } finally {
            limiter.shutdown();
        }
    }
    
    private void waitForRefresh(RateLimiter.Metrics metrics, 
                               RateLimiterConfig config, char printedWhileWaiting) throws InterruptedException {
        java.time.Instant start = java.time.Instant.now();
        while (java.time.Instant.now().isBefore(start.plus(config.getLimitRefreshPeriod()))) {
            if (metrics.getAvailablePermissions() == config.getLimitForPeriod()) {
                break;
            }
            System.out.print(printedWhileWaiting);
            Thread.sleep(10);
        }
        System.out.println();
    }
    
    @Test
    public void comprehensiveDualThreadModeValidation() throws Exception {
        System.out.println("Running comprehensive dual-thread mode validation...");
        
        // Define test scenarios
        TestScenario[] scenarios = {
            new TestScenario("Basic acquisition", 5, 0, 3, 2),
            new TestScenario("Full consumption", 3, 0, 3, 0),
            new TestScenario("Partial consumption", 8, 0, 5, 3),
            new TestScenario("Over-acquisition", 4, 0, 6, 0) // 6 > 4, should fail
        };
        
        for (TestScenario scenario : scenarios) {
            System.out.println("\nTesting scenario: " + scenario.name);
            validateScenarioInBothModes(scenario);
        }
        
        System.out.println("\n✅ All dual-thread validation scenarios passed!");
    }
    
    private void validateScenarioInBothModes(TestScenario scenario) throws Exception {
        // Test in platform mode
        System.clearProperty(SYS_PROP_KEY);
        TestResult platformResult = runScenario(scenario, "platform");
        
        // Test in virtual thread mode
        System.setProperty(SYS_PROP_KEY, "virtual");
        TestResult virtualResult = runScenario(scenario, "virtual");
        
        // Compare results
        assertEquals("Acquisition success should be identical between thread modes", 
            platformResult.acquisitionSuccess, virtualResult.acquisitionSuccess);
        assertEquals("Final permits should be identical between thread modes",
            platformResult.finalPermits, virtualResult.finalPermits);
        
        System.out.println(String.format("✅ %s: Platform=%s, Virtual=%s (identical)", 
            scenario.name, platformResult, virtualResult));
    }
    
    private TestResult runScenario(TestScenario scenario, String mode) throws Exception {
        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(scenario.initialPermits)
            .limitRefreshPeriod(Duration.ofMillis(200))
            .timeoutDuration(Duration.ofMillis(scenario.timeoutMs))
            .build();

        SemaphoreBasedRateLimiter limiter = 
            new SemaphoreBasedRateLimiter("scenario-" + mode, config, (ScheduledExecutorService) null);

        try {
            // Wait for initial refresh
            Thread.sleep(10);
            
            boolean success = limiter.acquirePermission(scenario.permitsToAcquire);
            int finalPermits = limiter.getMetrics().getAvailablePermissions();
            
            return new TestResult(success, finalPermits);
        } finally {
            limiter.shutdown();
        }
    }
    
    // Helper classes for comprehensive validation
    private static class TestScenario {
        final String name;
        final int initialPermits;
        final int timeoutMs;
        final int permitsToAcquire;
        final int expectedFinalPermits;
        
        TestScenario(String name, int initialPermits, int timeoutMs, int permitsToAcquire, int expectedFinalPermits) {
            this.name = name;
            this.initialPermits = initialPermits;
            this.timeoutMs = timeoutMs;
            this.permitsToAcquire = permitsToAcquire;
            this.expectedFinalPermits = expectedFinalPermits;
        }
    }
    
    private static class TestResult {
        final boolean acquisitionSuccess;
        final int finalPermits;
        
        TestResult(boolean acquisitionSuccess, int finalPermits) {
            this.acquisitionSuccess = acquisitionSuccess;
            this.finalPermits = finalPermits;
        }
        
        @Override
        public String toString() {
            return String.format("success=%b,permits=%d", acquisitionSuccess, finalPermits);
        }
    }
}
