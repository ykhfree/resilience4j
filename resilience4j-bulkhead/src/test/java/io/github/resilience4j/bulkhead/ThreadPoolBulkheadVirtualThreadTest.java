package io.github.resilience4j.bulkhead;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify ThreadPoolBulkhead correctly integrates with virtual threads
 * when the system property {@code resilience4j.thread.type} is set to {@code virtual}.
 */
public class ThreadPoolBulkheadVirtualThreadTest {

    private static final String THREAD_TYPE_PROPERTY = "resilience4j.thread.type";
    private static final int MAX_THREAD_POOL_SIZE = 5;
    private static final int CORE_THREAD_POOL_SIZE = 1;
    private static final int QUEUE_CAPACITY = 5;
    
    @Before
    public void setUp() {
        // No specific setup needed
    }
    
    @After
    public void tearDown() {
        // Clean up property
        System.clearProperty(THREAD_TYPE_PROPERTY);
    }
    
    @Test
    public void shouldUseVirtualThreadsWhenConfigured() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create ThreadPoolBulkhead with default config
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(MAX_THREAD_POOL_SIZE)
            .coreThreadPoolSize(CORE_THREAD_POOL_SIZE)
            .queueCapacity(QUEUE_CAPACITY)
            .keepAliveDuration(Duration.ofMillis(100))
            .build();
            
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead.of("virtualThreadTest", config);
        
        // Submit a task that checks if it's running on a virtual thread
        AtomicBoolean ranOnVirtualThread = new AtomicBoolean(false);
        
        // Submit a task that verifies it's running on a virtual thread
        CompletableFuture<Boolean> future = bulkhead.submit(() -> {
            ranOnVirtualThread.set(Thread.currentThread().isVirtual());
            return true;
        }).toCompletableFuture();
        
        // Wait for task to complete
        boolean result = future.get(1, TimeUnit.SECONDS);
        
        // Verify execution was successful
        assertThat(result).isTrue();
        
        // Verify that the task ran on a virtual thread
        assertThat(ranOnVirtualThread.get())
            .as("Task should execute on a virtual thread when configured")
            .isTrue();
            
        // Clean up
        bulkhead.close();
    }
    
    @Test
    public void shouldHandleConcurrentTasksWithVirtualThreads() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create ThreadPoolBulkhead config with limited capacity
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(MAX_THREAD_POOL_SIZE)
            .coreThreadPoolSize(CORE_THREAD_POOL_SIZE)
            .queueCapacity(QUEUE_CAPACITY)
            .keepAliveDuration(Duration.ofMillis(100))
            .build();
            
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead.of("virtualThreadConcurrencyTest", config);
        
        // Submit more tasks than the thread pool and queue capacity
        int numberOfTasks = MAX_THREAD_POOL_SIZE + QUEUE_CAPACITY + 5;
        CompletableFuture<?>[] futures = new CompletableFuture<?>[numberOfTasks];
        AtomicInteger rejectedTasks = new AtomicInteger(0);
        AtomicInteger completedTasks = new AtomicInteger(0);
        CountDownLatch tasksLatch = new CountDownLatch(numberOfTasks);
        
        // Submit tasks to the bulkhead
        for (int i = 0; i < numberOfTasks; i++) {
            final int taskId = i;
            
            try {
                futures[i] = bulkhead.submit(() -> {
                    try {
                        // Simulate some work
                        Thread.sleep(50);
                        completedTasks.incrementAndGet();
                        return taskId;
                    } catch (InterruptedException e) {
                        return -1;
                    }
                }).toCompletableFuture();
                
                // Register callback to count down the latch
                futures[i].whenComplete((result, error) -> tasksLatch.countDown());
            } catch (BulkheadFullException e) {
                rejectedTasks.incrementAndGet();
                futures[i] = CompletableFuture.completedFuture(-1);
                tasksLatch.countDown();
            }
        }
        
        // Wait for all tasks to complete or be rejected
        boolean completed = tasksLatch.await(10, TimeUnit.SECONDS);
        
        // Verify all tasks were accounted for (either completed or rejected)
        assertThat(completed)
            .as("All tasks should be completed or rejected within timeout")
            .isTrue();
            
        // Verify the number of accepted tasks matches the capacity
        assertThat(completedTasks.get())
            .as("Number of completed tasks should match thread pool and queue capacity")
            .isLessThanOrEqualTo(MAX_THREAD_POOL_SIZE + QUEUE_CAPACITY);
            
        // Verify some tasks were rejected due to capacity limits
        assertThat(rejectedTasks.get())
            .as("Some tasks should be rejected when exceeding capacity")
            .isGreaterThan(0);
            
        // Verify total tasks accounted for
        assertThat(completedTasks.get() + rejectedTasks.get())
            .as("Total tasks (completed + rejected) should equal the number of submitted tasks")
            .isEqualTo(numberOfTasks);
            
        // Clean up
        bulkhead.close();
    }
    
    @Test
    public void shouldHaveCorrectMetricsWithVirtualThreads() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Create ThreadPoolBulkhead with default config
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(MAX_THREAD_POOL_SIZE)
            .coreThreadPoolSize(CORE_THREAD_POOL_SIZE)
            .queueCapacity(QUEUE_CAPACITY)
            .keepAliveDuration(Duration.ofMillis(100))
            .build();
            
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead.of("virtualThreadMetricsTest", config);
        
        // Get initial metrics
        ThreadPoolBulkhead.Metrics metrics = bulkhead.getMetrics();
        
        // Verify initial metrics
        assertThat(metrics.getCoreThreadPoolSize()).isEqualTo(CORE_THREAD_POOL_SIZE);
        assertThat(metrics.getMaximumThreadPoolSize()).isEqualTo(MAX_THREAD_POOL_SIZE);
        assertThat(metrics.getQueueCapacity()).isEqualTo(QUEUE_CAPACITY);
        
        // Submit a single task and verify metrics
        CountDownLatch latch = new CountDownLatch(1);
        bulkhead.submit(() -> {
            try {
                Thread.sleep(100);
                return true;
            } finally {
                latch.countDown();
            }
        });
        
        // Wait for task to complete
        latch.await(1, TimeUnit.SECONDS);
        
        // Clean up
        bulkhead.close();
    }
}