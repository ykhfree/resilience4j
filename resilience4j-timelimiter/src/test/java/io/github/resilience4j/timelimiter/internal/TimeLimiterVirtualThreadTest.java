package io.github.resilience4j.timelimiter.internal;

import io.github.resilience4j.core.ExecutorServiceFactory;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify TimeLimiter correctly uses virtual threads
 * when the system property {@code resilience4j.thread.type} is set to {@code virtual}.
 */
public class TimeLimiterVirtualThreadTest {

    private static final String THREAD_TYPE_PROPERTY = "resilience4j.thread.type";
    private static final Duration TIMEOUT = Duration.ofMillis(1000);
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() {
        // Reset the scheduler for each test
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @After
    public void tearDown() {
        // Clean up property and scheduler
        System.clearProperty(THREAD_TYPE_PROPERTY);
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void shouldUseVirtualThreadsWhenConfigured() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create scheduler via ExecutorServiceFactory which should use virtual threads
        scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor("timelimiter-vt-test");
        
        // Create TimeLimiter
        TimeLimiter timeLimiter = TimeLimiter.of(TimeLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .build());
        
        // Setup a task to check if it's running on a virtual thread
        AtomicBoolean ranOnVirtualThread = new AtomicBoolean(false);
        
        // Use Executors.newVirtualThreadPerTaskExecutor to ensure our task runs on a virtual thread
        ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            try {
                // Simulate some work
                Thread.sleep(50);
                ranOnVirtualThread.set(Thread.currentThread().isVirtual());
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        }, virtualExecutor);
        
        // Decorate with TimeLimiter
        Supplier<CompletionStage<Boolean>> decoratedSupplier = timeLimiter.decorateCompletionStage(
            scheduler, () -> future);
        
        // Execute and get result
        Boolean result = decoratedSupplier.get().toCompletableFuture().get(2, TimeUnit.SECONDS);
        
        // Verify execution was successful
        assertThat(result).isTrue();
        
        // Verify that the timeout handling uses a virtual thread
        // This is done by submitting a task to the scheduler and checking thread type
        CompletableFuture<Boolean> threadTypeFuture = new CompletableFuture<>();
        scheduler.execute(() -> {
            boolean isVirtual = Thread.currentThread().isVirtual();
            threadTypeFuture.complete(isVirtual);
        });
        
        // Verify the scheduler used virtual threads
        Boolean isVirtualThread = threadTypeFuture.get(1, TimeUnit.SECONDS);
        assertThat(isVirtualThread)
            .as("TimeLimiter's scheduler should use virtual threads when configured")
            .isTrue();
            
        // Also verify our test ran on a virtual thread
        assertThat(ranOnVirtualThread.get())
            .as("CompletableFuture execution should run on a virtual thread")
            .isTrue();
    }

    @Test
    public void shouldUsePlatformThreadsByDefault() throws Exception {
        // Don't set system property - should use platform threads by default
        
        // Create scheduler via ExecutorServiceFactory which should use platform threads
        scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor("timelimiter-pt-test");
        
        // Create TimeLimiter
        TimeLimiter timeLimiter = TimeLimiter.of(TimeLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .build());
            
        // Setup a task that will complete successfully
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(50);
                return "Success";
            } catch (InterruptedException e) {
                return "Interrupted";
            }
        });
        
        // Decorate with TimeLimiter
        Supplier<CompletionStage<String>> decoratedSupplier = timeLimiter.decorateCompletionStage(
            scheduler, () -> future);
        
        // Execute and get result
        String result = decoratedSupplier.get().toCompletableFuture().get(2, TimeUnit.SECONDS);
        
        // Verify execution was successful
        assertThat(result).isEqualTo("Success");
        
        // Verify that the timeout handling uses a platform thread
        CompletableFuture<Boolean> threadTypeFuture = new CompletableFuture<>();
        scheduler.execute(() -> {
            boolean isVirtual = Thread.currentThread().isVirtual();
            threadTypeFuture.complete(isVirtual);
        });
        
        // Verify the scheduler used platform threads
        Boolean isVirtualThread = threadTypeFuture.get(1, TimeUnit.SECONDS);
        assertThat(isVirtualThread)
            .as("TimeLimiter's scheduler should use platform threads by default")
            .isFalse();
    }

    @Test
    public void shouldTimeoutAndCancelOnVirtualThread() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create a CountDownLatch to track if our task was interrupted
        CountDownLatch interruptedLatch = new CountDownLatch(1);
        
        // Create executor service for the test
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            // Create TimeLimiter with short timeout
            TimeLimiter timeLimiter = TimeLimiter.of(TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(50))
                .cancelRunningFuture(true)
                .build());
            
            // Create a blocking callable that will run longer than our timeout
            Callable<String> longRunningTask = () -> {
                try {
                    Thread.sleep(10000); // Sleep for 10 seconds
                    return "Task completed";
                } catch (InterruptedException e) {
                    interruptedLatch.countDown(); // Signal that we were interrupted
                    throw e; // Rethrow to properly handle interruption
                }
            };
            
            // Submit the callable to get a cancellable future
            Future<String> future = executor.submit(longRunningTask);
            
            // Now use the TimeLimiter directly on this Future
            try {
                // This should timeout and cancel the future
                timeLimiter.decorateFutureSupplier(() -> future).call();
                
                // Should not reach here
                throw new AssertionError("Expected timeout exception");
            } catch (TimeoutException e) {
                // Expected - timeout occurred
                
                // Wait for the interruption to propagate
                boolean wasInterrupted = interruptedLatch.await(500, TimeUnit.MILLISECONDS);
                
                // Verify the task was interrupted
                assertThat(wasInterrupted)
                    .as("Task should have been interrupted due to cancellation")
                    .isTrue();
                
                // Also verify the future was cancelled
                assertThat(future.isCancelled())
                    .as("Future should have been cancelled")
                    .isTrue();
            }
        } finally {
            executor.shutdownNow();
        }
    }
}