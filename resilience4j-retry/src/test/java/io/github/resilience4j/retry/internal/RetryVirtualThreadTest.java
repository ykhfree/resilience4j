package io.github.resilience4j.retry.internal;

import io.github.resilience4j.core.ExecutorServiceFactory;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify Retry correctly uses virtual threads
 * when the system property {@code resilience4j.thread.type} is set to {@code virtual}.
 */
public class RetryVirtualThreadTest {

    private static final String THREAD_TYPE_PROPERTY = "resilience4j.thread.type";
    private static final Duration WAIT_DURATION = Duration.ofMillis(50);
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
    public void shouldUseVirtualThreadsForAsyncRetry() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create scheduler via ExecutorServiceFactory which should use virtual threads
        scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor("retry-vt-test");
        
        // Create Retry with configuration to retry 3 times
        RetryConfig config = RetryConfig.<String>custom()
            .maxAttempts(3)
            .waitDuration(WAIT_DURATION)
            .build();
        Retry retry = Retry.of("virtualThreadTest", config);
        
        // Track retry attempts and thread types
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicBoolean allVirtualThreads = new AtomicBoolean(true);
        
        // Create a supplier that will fail twice then succeed, tracking thread types
        // Use a virtual thread executor for the supplier
        ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        
        Supplier<CompletionStage<String>> supplier = () -> {
            // Submit the actual work to a virtual thread
            return CompletableFuture.supplyAsync(() -> {
                int currentAttempt = attempts.incrementAndGet();
                
                // Track if we're running on a virtual thread
                if (!Thread.currentThread().isVirtual()) {
                    allVirtualThreads.set(false);
                }
                
                // Return success or throw exception based on attempt count
                if (currentAttempt < 3) {
                    // First two attempts fail
                    throw new RuntimeException("Retry attempt: " + currentAttempt);
                } else {
                    // Third attempt succeeds
                    return "Success on attempt: " + currentAttempt;
                }
            }, virtualExecutor);
        };
        
        // Decorate the supplier with Retry
        Supplier<CompletionStage<String>> decoratedSupplier = Retry.decorateCompletionStage(
            retry, scheduler, supplier);
        
        // Execute and get result
        CompletionStage<String> stage = decoratedSupplier.get();
        String result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
        
        // Verify we retried the correct number of times
        assertThat(attempts.get()).isEqualTo(3);
        
        // Verify the result is correct
        assertThat(result).isEqualTo("Success on attempt: 3");
        
        // Verify all executions used virtual threads
        assertThat(allVirtualThreads.get())
            .as("All retry attempts should run on virtual threads")
            .isTrue();
            
        // Verify the scheduler is using virtual threads
        CompletableFuture<Boolean> threadTypeFuture = new CompletableFuture<>();
        scheduler.execute(() -> {
            boolean isVirtual = Thread.currentThread().isVirtual();
            threadTypeFuture.complete(isVirtual);
        });
        
        Boolean isVirtualThread = threadTypeFuture.get(1, TimeUnit.SECONDS);
        assertThat(isVirtualThread)
            .as("Retry's scheduler should use virtual threads when configured")
            .isTrue();
    }
    
    @Test
    public void shouldUsePlatformThreadsByDefault() throws Exception {
        // Don't set system property - should use platform threads by default
        
        // Create scheduler via ExecutorServiceFactory which should use platform threads
        scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor("retry-pt-test");
        
        // Create Retry with configuration to retry once
        RetryConfig config = RetryConfig.<String>custom()
            .maxAttempts(2)
            .waitDuration(WAIT_DURATION)
            .build();
        Retry retry = Retry.of("platformThreadTest", config);
        
        // Track retry attempts and thread types
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicBoolean anyVirtualThread = new AtomicBoolean(false);
        
        // Create a supplier that will fail then succeed, tracking thread types
        Supplier<CompletionStage<String>> supplier = () -> {
            int currentAttempt = attempts.incrementAndGet();
            
            // Track if we're running on a virtual thread
            if (Thread.currentThread().isVirtual()) {
                anyVirtualThread.set(true);
            }
            
            CompletableFuture<String> future = new CompletableFuture<>();
            
            if (currentAttempt == 1) {
                // First attempt fails
                future.completeExceptionally(new RuntimeException("Retry attempt: " + currentAttempt));
            } else {
                // Second attempt succeeds
                future.complete("Success on attempt: " + currentAttempt);
            }
            
            return future;
        };
        
        // Decorate the supplier with Retry
        Supplier<CompletionStage<String>> decoratedSupplier = Retry.decorateCompletionStage(
            retry, scheduler, supplier);
        
        // Execute and get result
        CompletionStage<String> stage = decoratedSupplier.get();
        String result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
        
        // Verify we retried the correct number of times
        assertThat(attempts.get()).isEqualTo(2);
        
        // Verify the result is correct
        assertThat(result).isEqualTo("Success on attempt: 2");
        
        // Verify no executions used virtual threads
        assertThat(anyVirtualThread.get())
            .as("No retry attempts should run on virtual threads by default")
            .isFalse();
            
        // Verify the scheduler is using platform threads
        CompletableFuture<Boolean> threadTypeFuture = new CompletableFuture<>();
        scheduler.execute(() -> {
            boolean isVirtual = Thread.currentThread().isVirtual();
            threadTypeFuture.complete(isVirtual);
        });
        
        Boolean isVirtualThread = threadTypeFuture.get(1, TimeUnit.SECONDS);
        assertThat(isVirtualThread)
            .as("Retry's scheduler should use platform threads by default")
            .isFalse();
    }
    
    @Test
    public void shouldHandleHighConcurrencyWithVirtualThreads() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create scheduler via ExecutorServiceFactory which should use virtual threads
        scheduler = ExecutorServiceFactory.newSingleThreadScheduledExecutor("retry-vt-concurrency-test");
        
        // Create Retry with configuration for high concurrency testing
        RetryConfig config = RetryConfig.<String>custom()
            .maxAttempts(3)
            .waitDuration(WAIT_DURATION)
            .build();
        Retry retry = Retry.of("virtualThreadConcurrencyTest", config);
        
        // Number of concurrent operations to test
        final int CONCURRENT_TASKS = 100;
        
        // Track completion of all tasks
        CountDownLatch completionLatch = new CountDownLatch(CONCURRENT_TASKS);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failureCounter = new AtomicInteger(0);
        
        // Create and execute concurrent tasks
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < CONCURRENT_TASKS; i++) {
                final int taskId = i;
                
                executor.submit(() -> {
                    try {
                        // Each task retries up to 3 times with different outcomes based on taskId
                        AtomicInteger attemptCounter = new AtomicInteger(0);
                        
                        // Create a supplier with different behaviors based on taskId
                        Supplier<CompletionStage<String>> supplier = () -> {
                            int attempt = attemptCounter.incrementAndGet();
                            CompletableFuture<String> future = new CompletableFuture<>();
                            
                            // Different behavior based on task ID
                            // - Even tasks succeed on first try
                            // - Odd tasks < 50 succeed on second try
                            // - Odd tasks >= 50 succeed on third try
                            if (taskId % 2 == 0) {
                                // Even tasks succeed immediately
                                future.complete("Task " + taskId + " succeeded on attempt " + attempt);
                            } else if (taskId < 50) {
                                // Odd tasks < 50 succeed on second attempt
                                if (attempt >= 2) {
                                    future.complete("Task " + taskId + " succeeded on attempt " + attempt);
                                } else {
                                    future.completeExceptionally(new RuntimeException("Retry needed for task " + taskId));
                                }
                            } else {
                                // Odd tasks >= 50 succeed on third attempt
                                if (attempt >= 3) {
                                    future.complete("Task " + taskId + " succeeded on attempt " + attempt);
                                } else {
                                    future.completeExceptionally(new RuntimeException("Retry needed for task " + taskId));
                                }
                            }
                            
                            return future;
                        };
                        
                        // Decorate supplier with retry
                        Supplier<CompletionStage<String>> decoratedSupplier = Retry.decorateCompletionStage(
                            retry, scheduler, supplier);
                            
                        // Execute and wait for result
                        String result = decoratedSupplier.get().toCompletableFuture().get(5, TimeUnit.SECONDS);
                        
                        // Task succeeded
                        successCounter.incrementAndGet();
                    } catch (Exception e) {
                        // Task failed even after retries
                        failureCounter.incrementAndGet();
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            // Wait for all tasks to complete
            boolean completed = completionLatch.await(10, TimeUnit.SECONDS);
            
            // Verify all tasks completed
            assertThat(completed)
                .as("All concurrent retry tasks should complete within timeout")
                .isTrue();
                
            // Verify all tasks succeeded
            assertThat(successCounter.get())
                .as("All tasks should eventually succeed with retries")
                .isEqualTo(CONCURRENT_TASKS);
                
            assertThat(failureCounter.get())
                .as("No tasks should fail")
                .isZero();
        }
    }
}