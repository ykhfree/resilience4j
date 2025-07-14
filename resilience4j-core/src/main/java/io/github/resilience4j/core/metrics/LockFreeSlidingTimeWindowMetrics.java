/*
 *
 *  Copyright 2024 Florentin Simion and Rares Vlasceanu
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
package io.github.resilience4j.core.metrics;

import io.github.resilience4j.core.Clock;
import io.github.resilience4j.core.ExecutorServiceFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Virtual thread compatible time-based sliding window metrics implementation that delegates to appropriate
 * implementation based on thread type:
 * - Platform threads: Uses lock-free algorithm with CAS operations for maximum performance
 * - Virtual threads: Uses ReentrantLock-based algorithm to avoid carrier thread pinning
 * 
 * The time sliding window represents aggregated stats across 1-second time slices.
 * For virtual threads, it uses ReentrantLock to prevent carrier thread pinning while maintaining good performance.
 */
public class LockFreeSlidingTimeWindowMetrics implements Metrics {
    
    private final Metrics delegate;

    public LockFreeSlidingTimeWindowMetrics(int windowSize, Clock clock) {
        // Delegate to appropriate implementation based on thread type
        if (ExecutorServiceFactory.useVirtualThreads()) {
            this.delegate = new ReentrantLockBasedMetrics(windowSize, clock);
        } else {
            this.delegate = new CASBasedMetrics(windowSize, clock);
        }
    }

    public LockFreeSlidingTimeWindowMetrics(int windowSize) {
        this(windowSize, Clock.SYSTEM);
    }

    @Override
    public Snapshot record(long duration, TimeUnit durationUnit, Outcome outcome) {
        return delegate.record(duration, durationUnit, outcome);
    }

    @Override
    public Snapshot getSnapshot() {
        return delegate.getSnapshot();
    }

    /**
     * CAS-based implementation for platform threads - preserves original lock-free algorithm
     */
    private static class CASBasedMetrics implements Metrics {
        private static final long TIME_SLICE_DURATION_IN_NANOS = TimeUnit.SECONDS.toNanos(1);
        private static final VarHandle HEAD;
        private static final VarHandle TAIL;
        private static final VarHandle TIME_SLICE;
        private static final VarHandle NEXT;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                HEAD = l.findVarHandle(CASBasedMetrics.class, "headRef", Node.class);
                TAIL = l.findVarHandle(CASBasedMetrics.class, "tailRef", Node.class);
                TIME_SLICE = l.findVarHandle(Node.class, "timeSlice", TimeSlice.class);
                NEXT = l.findVarHandle(Node.class, "next", Node.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final Clock clock;
        private final int windowSize;
        private volatile Node headRef;
        private volatile Node tailRef;

        CASBasedMetrics(int windowSize, Clock clock) {
            long time = clock.monotonicTime();
            this.clock = clock;
            this.windowSize = windowSize;
            this.headRef = new Node(new TimeSlice(0, time, new PackedAggregation(), false), null);
            this.tailRef = headRef;

            for (int i = 1; i < this.windowSize; i++) {
                Node newNode = new Node(new TimeSlice(i, time, new PackedAggregation(), false), null);
                tailRef.next = newNode;
                tailRef = newNode;
            }
        }

        @Override
        public Snapshot record(long duration, TimeUnit durationUnit, Outcome outcome) {
            while (true) {
                advanceTimeSlice();
                Node tail = tailRef;
                TimeSlice current = tail.timeSlice;

                if (current.processed) continue;

                TimeSlice next = current.copy();
                next.record(duration, durationUnit, outcome);

                if (TIME_SLICE.compareAndSet(tail, current, next)) {
                    return new SnapshotImpl(next.stats);
                }
            }
        }

        @Override
        public Snapshot getSnapshot() {
            advanceTimeSlice();
            return new SnapshotImpl(tailRef.timeSlice.stats);
        }

        private void advanceTimeSlice() {
            while (true) {
                Node tail = tailRef;
                TimeSlice current = tail.timeSlice;
                long now = clock.monotonicTime();
                long elapsedTime = now - current.time;

                if (elapsedTime < TIME_SLICE_DURATION_IN_NANOS) return;

                if (!current.processed) {
                    TimeSlice processed = new TimeSlice(current.second, current.time, current.stats, true);
                    if (TIME_SLICE.compareAndSet(tail, current, processed)) {
                        current = processed;
                    } else {
                        continue;
                    }
                }

                long elapsedSlices = Math.min(elapsedTime / TIME_SLICE_DURATION_IN_NANOS, windowSize);
                long elapsedTimeInNextSlice = elapsedTime - (elapsedSlices * TIME_SLICE_DURATION_IN_NANOS);
                if (elapsedTimeInNextSlice >= TIME_SLICE_DURATION_IN_NANOS) elapsedTimeInNextSlice = 0;

                int nextSecond = (current.second + 1) % windowSize;
                long nextTime = now - (elapsedSlices - 1) * TIME_SLICE_DURATION_IN_NANOS - elapsedTimeInNextSlice;
                updateWindow(nextSecond, nextTime);
            }
        }

        private void updateWindow(int second, long time) {
            while (true) {
                Node head = headRef;
                Node headNext = head.next;
                Node tail = tailRef;
                Node tailNext = tail.next;

                if (head != headRef || tail != tailRef) continue;

                TimeSlice headTimeSlice = head.timeSlice;
                TimeSlice tailTimeSlice = tail.timeSlice;
                int nextSecond = (tailTimeSlice.second + 1) % windowSize;

                if (second != nextSecond || time < tailTimeSlice.time) return;

                if (tailNext == null) {
                    PackedAggregation nextStats = tailTimeSlice.stats.copy();
                    nextStats.discard(headTimeSlice.stats);
                    TimeSlice nextTimeSlice = new TimeSlice(nextSecond, time, nextStats, false);
                    Node nextNode = new Node(nextTimeSlice, null);

                    if (NEXT.compareAndSet(tail, null, nextNode)) {
                        if (HEAD.compareAndSet(this, head, headNext)) {
                            TAIL.compareAndSet(this, tail, nextNode);
                            return;
                        }
                    }
                } else if (tailNext.timeSlice.second == headTimeSlice.second) {
                    if (HEAD.compareAndSet(this, head, headNext)) {
                        TAIL.compareAndSet(this, tail, tailNext);
                    }
                } else {
                    TAIL.compareAndSet(this, tail, tailNext);
                }
            }
        }

        public static class TimeSlice {
            final int second;
            final long time;
            final PackedAggregation stats;
            final boolean processed;

            public TimeSlice(int second, long time, PackedAggregation stats, boolean processed) {
                this.second = second;
                this.time = time;
                this.stats = stats;
                this.processed = processed;
            }

            public TimeSlice copy() {
                return new TimeSlice(second, time, stats.copy(), processed);
            }

            public void record(Long duration, TimeUnit durationUnit, Outcome outcome) {
                stats.record(duration, durationUnit, outcome);
            }
        }

        private static class Node {
            volatile TimeSlice timeSlice;
            volatile Node next;

            Node(TimeSlice timeSlice, Node next) {
                TIME_SLICE.set(this, timeSlice);
                NEXT.set(this, next);
            }
        }
    }

    /**
     * ReentrantLock-based implementation for virtual threads - avoids carrier thread pinning
     */
    private static class ReentrantLockBasedMetrics implements Metrics {
        private static final long TIME_SLICE_DURATION_IN_NANOS = TimeUnit.SECONDS.toNanos(1);
        private final ReentrantLock lock = new ReentrantLock();
        private final Clock clock;
        private final int windowSize;
        private Node headRef;
        private Node tailRef;

        ReentrantLockBasedMetrics(int windowSize, Clock clock) {
            long time = clock.monotonicTime();
            this.clock = clock;
            this.windowSize = windowSize;
            this.headRef = new Node(new TimeSlice(0, time, new PackedAggregation(), false), null);
            this.tailRef = headRef;

            for (int i = 1; i < this.windowSize; i++) {
                Node newNode = new Node(new TimeSlice(i, time, new PackedAggregation(), false), null);
                tailRef.next = newNode;
                tailRef = newNode;
            }
        }

        @Override
        public Snapshot record(long duration, TimeUnit durationUnit, Outcome outcome) {
            lock.lock();
            try {
                advanceTimeSlice();
                Node tail = tailRef;
                TimeSlice current = tail.timeSlice;

                if (current.processed) {
                    // Need to retry with fresh time slice
                    advanceTimeSlice();
                    tail = tailRef;
                    current = tail.timeSlice;
                }

                TimeSlice next = current.copy();
                next.record(duration, durationUnit, outcome);
                tail.timeSlice = next;

                return new SnapshotImpl(next.stats);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Snapshot getSnapshot() {
            // Simple lock-based approach for virtual thread compatibility
            lock.lock();
            try {
                advanceTimeSlice();
                Node tail = tailRef;
                TimeSlice timeSlice = tail != null ? tail.timeSlice : null;
                return new SnapshotImpl(timeSlice.stats);
            } finally {
                lock.unlock();
            }
        }

        private void advanceTimeSlice() {
            Node tail = tailRef;
            TimeSlice current = tail.timeSlice;
            long now = clock.monotonicTime();
            long elapsedTime = now - current.time;

            if (elapsedTime < TIME_SLICE_DURATION_IN_NANOS) return;

            if (!current.processed) {
                current = new TimeSlice(current.second, current.time, current.stats, true);
                tail.timeSlice = current;
            }

            long elapsedSlices = Math.min(elapsedTime / TIME_SLICE_DURATION_IN_NANOS, windowSize);
            long elapsedTimeInNextSlice = elapsedTime - (elapsedSlices * TIME_SLICE_DURATION_IN_NANOS);
            if (elapsedTimeInNextSlice >= TIME_SLICE_DURATION_IN_NANOS) elapsedTimeInNextSlice = 0;

            int nextSecond = (current.second + 1) % windowSize;
            long nextTime = now - (elapsedSlices - 1) * TIME_SLICE_DURATION_IN_NANOS - elapsedTimeInNextSlice;
            updateWindow(nextSecond, nextTime);
        }

        private void updateWindow(int second, long time) {
            Node head = headRef;
            Node tail = tailRef;
            TimeSlice headTimeSlice = head.timeSlice;
            TimeSlice tailTimeSlice = tail.timeSlice;
            int nextSecond = (tailTimeSlice.second + 1) % windowSize;

            if (second != nextSecond || time < tailTimeSlice.time) return;

            PackedAggregation nextStats = tailTimeSlice.stats.copy();
            nextStats.discard(headTimeSlice.stats);
            TimeSlice nextTimeSlice = new TimeSlice(nextSecond, time, nextStats, false);
            Node nextNode = new Node(nextTimeSlice, null);

            tail.next = nextNode;
            headRef = head.next;
            tailRef = nextNode;
        }

        public static class TimeSlice {
            final int second;
            final long time;
            final PackedAggregation stats;
            final boolean processed;

            public TimeSlice(int second, long time, PackedAggregation stats, boolean processed) {
                this.second = second;
                this.time = time;
                this.stats = stats;
                this.processed = processed;
            }

            public TimeSlice copy() {
                return new TimeSlice(second, time, stats.copy(), processed);
            }

            public void record(Long duration, TimeUnit durationUnit, Outcome outcome) {
                stats.record(duration, durationUnit, outcome);
            }
        }

        private static class Node {
            TimeSlice timeSlice;
            Node next;

            Node(TimeSlice timeSlice, Node next) {
                this.timeSlice = timeSlice;
                this.next = next;
            }
        }
    }
}
