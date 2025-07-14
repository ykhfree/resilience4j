/*
 *
 *  Copyright 2025
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for verifying the behavior of EventProcessor with virtual threads
 * and high concurrency. These tests specifically focus on thread pinning issues
 * and optimizations for virtual threads.
 */
public class EventProcessorVirtualThreadTest {

    private static final String THREAD_TYPE_PROPERTY = "resilience4j.thread.type";
    private static final int CONCURRENT_THREADS = 100;
    private static final int CONSUMERS_PER_THREAD = 10;
    
    private EventProcessor<TestEvent> eventProcessor;
    
    @Before
    public void setUp() {
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        eventProcessor = new EventProcessor<>();
    }
    
    @After
    public void tearDown() {
        System.clearProperty(THREAD_TYPE_PROPERTY);
    }
    
    /**
     * Test event class used for testing.
     */
    static class TestEvent {
        private final int id;
        
        TestEvent(int id) {
            this.id = id;
        }
        
        public int getId() {
            return id;
        }
    }
    
    @Test
    public void shouldHandleConcurrentRegistrationOfConsumers() throws Exception {
        // Counter to track total consumers registered
        AtomicInteger totalConsumers = new AtomicInteger(0);
        
        // Latch to coordinate threads starting together
        CountDownLatch startLatch = new CountDownLatch(1);
        
        // Latch to track completion of all threads
        CountDownLatch completionLatch = new CountDownLatch(CONCURRENT_THREADS);
        
        // Create threads and assign work
        try (var executor = Executors.newThreadPerTaskExecutor(
                 Thread.ofVirtual().name("event-processor-test-", 0).factory())) {
            
            // Launch concurrent threads to register consumers
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                final int threadNum = i;
                
                executor.submit(() -> {
                    try {
                        // Wait for all threads to be ready
                        startLatch.await();
                        
                        // Each thread registers multiple consumers
                        for (int j = 0; j < CONSUMERS_PER_THREAD; j++) {
                            int consumerId = threadNum * CONSUMERS_PER_THREAD + j;
                            
                            // Register a consumer that verifies its execution
                            eventProcessor.registerConsumer(TestEvent.class.getName(), 
                                event -> {
                                    // Just a simple consumer that counts events
                                    totalConsumers.incrementAndGet();
                                });
                        }
                        
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            // Start all threads
            startLatch.countDown();
            
            // Wait for all threads to complete registration
            boolean completed = completionLatch.await(10, TimeUnit.SECONDS);
            assertTrue("Timed out waiting for concurrent consumer registration", completed);
        }
        
        // Process an event to verify all consumers were registered
        boolean consumed = eventProcessor.processEvent(new TestEvent(1));
        
        // Verify processing results
        assertThat(consumed).isTrue();
        assertThat(totalConsumers.get()).isEqualTo(CONCURRENT_THREADS * CONSUMERS_PER_THREAD);
    }
    
    @Test
    public void shouldHandleConcurrentEventProcessing() throws Exception {
        // Set up counters
        AtomicInteger successfullyProcessed = new AtomicInteger(0);
        AtomicInteger virtualThreadCount = new AtomicInteger(0);
        
        // Add a bunch of consumers first
        for (int i = 0; i < CONSUMERS_PER_THREAD; i++) {
            eventProcessor.registerConsumer(TestEvent.class.getName(), event -> {
                // Record if running on a virtual thread
                if (Thread.currentThread().isVirtual()) {
                    virtualThreadCount.incrementAndGet();
                }
                
                // Simulate some processing work
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                // Count processed events
                successfullyProcessed.incrementAndGet();
            });
        }
        
        // Barrier to synchronize all threads
        CyclicBarrier barrier = new CyclicBarrier(CONCURRENT_THREADS);
        CountDownLatch completionLatch = new CountDownLatch(CONCURRENT_THREADS);
        
        // Track any exceptions during processing
        AtomicBoolean exceptionOccurred = new AtomicBoolean(false);
        
        try (var executor = Executors.newThreadPerTaskExecutor(
                 Thread.ofVirtual().name("event-processor-test-", 0).factory())) {
            
            // Launch concurrent threads to process events
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                final int threadNum = i;
                
                executor.submit(() -> {
                    try {
                        // Synchronize all threads to start at the same time
                        barrier.await(5, TimeUnit.SECONDS);
                        
                        // Process an event
                        TestEvent event = new TestEvent(threadNum);
                        eventProcessor.processEvent(event);
                        
                        return null;
                    } catch (Exception e) {
                        exceptionOccurred.set(true);
                        throw new RuntimeException(e);
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            // Wait for all threads to complete
            boolean completed = completionLatch.await(10, TimeUnit.SECONDS);
            assertTrue("Timed out waiting for concurrent event processing", completed);
            assertThat(exceptionOccurred).isFalse();
        }
        
        // Verify results
        assertThat(successfullyProcessed.get()).isEqualTo(CONCURRENT_THREADS * CONSUMERS_PER_THREAD);
        
        // Verify thread type - all events should be processed on virtual threads
        assertThat(virtualThreadCount.get()).isEqualTo(CONCURRENT_THREADS * CONSUMERS_PER_THREAD);
    }
    
    @Test
    public void shouldHandleConcurrentRegistrationAndProcessing() throws Exception {
        // Set up counters
        AtomicInteger eventsProcessed = new AtomicInteger(0);
        AtomicInteger consumersRegistered = new AtomicInteger(0);
        
        // Coordination latches
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(CONCURRENT_THREADS * 2); // 2x threads (consumer + processor)
        
        try (var executor = Executors.newThreadPerTaskExecutor(
                 Thread.ofVirtual().name("event-processor-test-", 0).factory())) {
            
            // Half the threads register consumers
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        // Register a consumer
                        eventProcessor.registerConsumer(TestEvent.class.getName(), event -> {
                            eventsProcessed.incrementAndGet();
                        });
                        
                        consumersRegistered.incrementAndGet();
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            // Half the threads process events
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                final int eventId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        
                        // Small delay to ensure some consumers are registered
                        Thread.sleep(5);
                        
                        // Process an event
                        eventProcessor.processEvent(new TestEvent(eventId));
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }
            
            // Start all threads
            startLatch.countDown();
            
            // Wait for all operations to complete
            boolean completed = completionLatch.await(10, TimeUnit.SECONDS);
            assertTrue("Timed out waiting for concurrent operations", completed);
        }
        
        // Verify all consumers were registered
        assertThat(consumersRegistered.get()).isEqualTo(CONCURRENT_THREADS);
        
        // Events processed may not equal consumers * events because
        // some events might have been processed before consumers were registered
        assertThat(eventsProcessed.get()).isGreaterThan(0);
        
        // Process one final event to verify all consumers are properly registered
        eventProcessor.processEvent(new TestEvent(CONCURRENT_THREADS + 1));
        assertThat(eventsProcessed.get()).isGreaterThanOrEqualTo(CONCURRENT_THREADS);
    }
}