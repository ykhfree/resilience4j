package io.github.resilience4j.circuitbreaker.internal;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Tests that verify CircuitBreakerStateMachine correctly uses virtual threads
 * when the system property {@code resilience4j.thread.type} is set to {@code virtual}.
 */
public class CircuitBreakerStateMachineVirtualThreadTest {

    private static final String THREAD_TYPE_PROPERTY = "resilience4j.thread.type";
    private static final Duration WAIT_DURATION = Duration.ofMillis(100);
    
    @Before
    public void setUp() {
        SchedulerFactory.getInstance().reset();
    }
    
    @After
    public void tearDown() {
        System.clearProperty(THREAD_TYPE_PROPERTY);
        SchedulerFactory.getInstance().reset();
    }
    
    @Test
    public void shouldUseVirtualThreadForAutomaticTransitionFromOpenToHalfOpen() throws Exception {
        // Configure to use virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Set up latch to wait for transition
        CountDownLatch transitionLatch = new CountDownLatch(1);
        AtomicBoolean isVirtualThread = new AtomicBoolean(false);
        
        // Create CircuitBreaker with automatic transition and a state change listener
        CircuitBreaker circuitBreaker = new CircuitBreakerStateMachine("testVirtualThread",
            CircuitBreakerConfig.custom()
                .waitDurationInOpenState(WAIT_DURATION)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
                
        // Add a state transition listener
        circuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                // Record if we're running in a virtual thread
                isVirtualThread.set(Thread.currentThread().isVirtual());
                transitionLatch.countDown();
            }
        });
        
        // Transition to OPEN state which will schedule automatic transition to HALF_OPEN
        circuitBreaker.transitionToOpenState();
        
        // Wait for automatic transition to HALF_OPEN
        assertTrue("Transition to HALF_OPEN did not occur within expected time", 
            transitionLatch.await(1, TimeUnit.SECONDS));
        
        // Verify that the transition happened on a virtual thread
        assertTrue("Automatic transition should have executed on a virtual thread", 
            isVirtualThread.get());
    }
    
    @Test
    public void shouldUsePlatformThreadByDefaultForAutomaticTransitionFromOpenToHalfOpen() throws Exception {
        // No system property set - should use platform threads by default
        
        // Set up latch to wait for transition
        CountDownLatch transitionLatch = new CountDownLatch(1);
        AtomicBoolean isVirtualThread = new AtomicBoolean(false);
        
        // Create CircuitBreaker with automatic transition and a state change listener
        CircuitBreaker circuitBreaker = new CircuitBreakerStateMachine("testPlatformThread",
            CircuitBreakerConfig.custom()
                .waitDurationInOpenState(WAIT_DURATION)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
                
        // Add a state transition listener
        circuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                // Record if we're running in a virtual thread
                isVirtualThread.set(Thread.currentThread().isVirtual());
                transitionLatch.countDown();
            }
        });
        
        // Transition to OPEN state which will schedule automatic transition to HALF_OPEN
        circuitBreaker.transitionToOpenState();
        
        // Wait for automatic transition to HALF_OPEN
        assertTrue("Transition to HALF_OPEN did not occur within expected time", 
            transitionLatch.await(1, TimeUnit.SECONDS));
        
        // Verify that the transition happened on a platform thread (not virtual)
        assertFalse("Automatic transition should have executed on a platform thread", 
            isVirtualThread.get());
    }
    
    @Test
    public void shouldRespectConfigChangesForThreadType() throws Exception {
        // Start with virtual threads
        System.setProperty(THREAD_TYPE_PROPERTY, "virtual");
        
        // Set up latches for transitions
        CountDownLatch firstTransitionLatch = new CountDownLatch(1);
        CountDownLatch secondTransitionLatch = new CountDownLatch(1);
        
        AtomicBoolean firstThreadIsVirtual = new AtomicBoolean(false);
        AtomicBoolean secondThreadIsVirtual = new AtomicBoolean(false);
        
        // Create first CircuitBreaker with automatic transition
        CircuitBreaker firstCircuitBreaker = new CircuitBreakerStateMachine("testVirtualThread",
            CircuitBreakerConfig.custom()
                .waitDurationInOpenState(WAIT_DURATION)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
                
        // Add a state transition listener to the first circuit breaker
        firstCircuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                firstThreadIsVirtual.set(Thread.currentThread().isVirtual());
                firstTransitionLatch.countDown();
            }
        });
        
        // Transition first CB to OPEN state which will schedule automatic transition to HALF_OPEN
        firstCircuitBreaker.transitionToOpenState();
        
        // Wait for first transition
        assertTrue("First transition to HALF_OPEN did not occur within expected time", 
            firstTransitionLatch.await(1, TimeUnit.SECONDS));
            
        // Verify first transition was on a virtual thread
        assertTrue("First transition should have executed on a virtual thread", 
            firstThreadIsVirtual.get());
            
        // Now switch to platform threads
        System.setProperty(THREAD_TYPE_PROPERTY, "platform");
        
        // Reset the scheduler to pick up new configuration
        SchedulerFactory.getInstance().reset();
        
        // Create second CircuitBreaker with automatic transition
        CircuitBreaker secondCircuitBreaker = new CircuitBreakerStateMachine("testSwitchToPlat",
            CircuitBreakerConfig.custom()
                .waitDurationInOpenState(WAIT_DURATION)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
                
        // Add a state transition listener to the second circuit breaker
        secondCircuitBreaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                secondThreadIsVirtual.set(Thread.currentThread().isVirtual());
                secondTransitionLatch.countDown();
            }
        });
        
        // Transition second CB to OPEN state which will schedule automatic transition to HALF_OPEN
        secondCircuitBreaker.transitionToOpenState();
        
        // Wait for second transition
        assertTrue("Second transition to HALF_OPEN did not occur within expected time", 
            secondTransitionLatch.await(1, TimeUnit.SECONDS));
            
        // Verify second transition was on a platform thread (not virtual)
        assertFalse("Second transition should have executed on a platform thread after property change", 
            secondThreadIsVirtual.get());
    }
}