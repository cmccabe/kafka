/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class KafkaEventQueue implements EventQueue {
    /**
     * A context object that wraps events.
     *
     * @param <T>       The type parameter of the wrapped event.
     */
    private static class EventContext<T> {
        /**
         * The caller-supplied event.
         */
        private final Event<T> event;

        /**
         * If non-null, the exception that we should deliver to all subsequent queued
         * events.
         */
        private final Throwable clearException;

        /**
         * The CompletableFuture that the caller can listen on.
         */
        private final CompletableFuture<T> future;

        /**
         * The previous pointer of our circular doubly-linked list.
         */
        private EventContext<?> prev = this;

        /**
         * The next pointer in our circular doubly-linked list.
         */
        private EventContext<?> next = this;

        /**
         * The time in monotonic nanoseconds when this even should be timed out, or
         * Long.MAX_VALUE if there is no timeout.
         */
        private long timeoutNs = Long.MAX_VALUE;

        EventContext(Event<T> event, Throwable clearException) {
            this.event = event;
            this.clearException = clearException;
            this.future = new CompletableFuture<>();
        }

        /**
         * Insert a new node in the circularly linked list after this node.
         */
        @SuppressWarnings("unchecked")
        void insertAfter(EventContext other) {
            this.next.prev = other;
            other.next = this.next;
            other.prev = this;
            this.next = other;
        }

        /**
         * Insert a new node in the circularly linked list before this node.
         */
        @SuppressWarnings("unchecked")
        void insertBefore(EventContext other) {
            this.prev.next = other;
            other.prev = this.prev;
            other.next = this;
            this.prev = other;
        }

        /**
         * Remove this node from the circularly linked list.
         */
        void remove() {
            this.prev.next = this.next;
            this.next.prev = this.prev;
            this.prev = this;
            this.next = this;
        }

        /**
         * Returns true if this node is the only element in its list.
         */
        boolean isSingleton() {
            return prev == this && next == this;
        }

        /**
         * Run the event associated with this EventContext.
         */
        void run() {
            try {
                future.complete(event.run());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        /**
         * Complete the event associated with this EventContext with a timeout exception.
         */
        void completeWithTimeout() {
            completeWithException(new TimeoutException());
        }

        /**
         * Complete the event associated with this EventContext with the specified
         * exception.
         */
        void completeWithException(Throwable t) {
            future.completeExceptionally(t);
        }
    }

    private class EventHandler implements Runnable {
        /**
         * The head of the event queue.
         */
        private final EventContext<Void> head = new EventContext<>(null, null);

        /**
         * A condition variable for waking up the event handler thread.
         */
        private final Condition cond = lock.newCondition();

        @Override
        public void run() {
            try {
                handleEvents();
            } catch (Throwable e) {
                log.warn("event handler thread exiting with exception", e);
            }
        }

        private void handleEvents() throws Throwable {
            while (true) {
                EventContext<?> eventContext;
                Throwable clearException = null;
                lock.lock();
                try {
                    while (head.isSingleton()) {
                        // If there are no more events to process, and the queue is
                        // shutting down, exit the event handler thread.
                        if (closingTimeNs != Long.MAX_VALUE) {
                            return;
                        }
                        cond.await();
                    }
                    eventContext = head.next;
                    if (eventContext.clearException != null && eventContext.next != head) {
                        // If there is a clearException set, complete all the subsequent
                        // events with it.
                        clearException = eventContext.clearException;
                        eventContext = eventContext.next;
                    }
                    eventContext.remove();
                    timeoutHandler.removeTimeout(eventContext);
                } finally {
                    lock.unlock();
                }
                // Drop the lock and complete the future.
                if (clearException == null) {
                    eventContext.run();
                } else {
                    eventContext.completeWithException(clearException);
                }
            }
        }

        void enqueue(boolean append, EventContext<?> eventContext) {
            lock.lock();
            try {
                boolean wasEmpty = head.isSingleton();
                if (append) {
                    head.insertBefore(eventContext);
                } else {
                    head.insertAfter(eventContext);
                }
                if (wasEmpty) {
                    // If the queue is going from empty to non-empty, then signal to
                    // make sure that the event handler thread will make progress.
                    cond.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private class TimeoutHandler implements Runnable {
        /**
         * An ordered map of times in monotonic nanoseconds to events to time out.
         */
        private final TreeMap<Long, EventContext<?>> timeouts = new TreeMap<>();

        /**
         * A condition variable for waking up the timeout handler thread.
         */
        private final Condition cond = lock.newCondition();

        @Override
        public void run() {
            try {
                handleTimeouts();
            } catch (Throwable e) {
                log.warn("timeout handler thread exiting with exception", e);
            }
        }

        private void handleTimeouts() throws Throwable {
            lock.lock();
            try {
                while (true) {
                    EventContext<?> toTimeout = null;
                    long nowNs = Time.SYSTEM.nanoseconds();
                    long toDelay = closingTimeNs - nowNs;
                    Map.Entry<Long, EventContext<?>> entry = timeouts.firstEntry();
                    if (entry != null) {
                        if (nowNs >= closingTimeNs || nowNs >= entry.getKey()) {
                            timeouts.remove(entry.getKey(), entry.getValue());
                            toTimeout = entry.getValue();
                        } else {
                            toDelay = Math.min(toDelay, entry.getKey() - nowNs);
                        }
                    } else {
                        // If there are no more entries in the timeout map, and the
                        // event queue is closing, the timeout handler thread should exit.
                        if (closingTimeNs != Long.MAX_VALUE) {
                            return;
                        }
                    }
                    if (toTimeout != null) {
                        // Unlink this element from its linked list, drop the lock, and
                        // complete it with an exception.
                        toTimeout.remove();
                        lock.unlock();
                        try {
                            toTimeout.completeWithTimeout();
                        } finally {
                            lock.lock();
                        }
                    } else {
                        cond.awaitNanos(toDelay);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        void setTimeout(EventContext eventContext, long timeoutNs) {
            lock.lock();
            try {
                if (eventContext.timeoutNs != Long.MAX_VALUE) {
                    throw new IllegalArgumentException("timeout was already set");
                }
                long prevStartNs = timeouts.isEmpty() ? Long.MAX_VALUE : timeouts.firstKey();
                // If the time in nanoseconds is already taken, take the next one.
                while (timeouts.putIfAbsent(timeoutNs, eventContext) != null) {
                    timeoutNs++;
                }
                eventContext.timeoutNs = timeoutNs;
                // If the new timeout is before all the existing ones, wake up the
                // timeout thread.
                if (timeoutNs <= prevStartNs) {
                    cond.signal();
                }
            } finally {
                lock.unlock();
            }
        }

        void removeTimeout(EventContext eventContext) {
            lock.lock();
            try {
                if (eventContext.timeoutNs != Long.MAX_VALUE) {
                    timeouts.remove(eventContext.timeoutNs, eventContext);
                    eventContext.timeoutNs = Long.MAX_VALUE;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private final Logger log;
    private final ReentrantLock lock = new ReentrantLock();
    private final EventHandler eventHandler;
    private final Thread eventHandlerThread;
    private final TimeoutHandler timeoutHandler;
    private final Thread timeoutHandlerThread;
    private long closingTimeNs = Long.MAX_VALUE;

    public KafkaEventQueue(LogContext logContext, String threadNamePrefix) {
        this.log = logContext.logger(KafkaEventQueue.class);
        this.eventHandler = new EventHandler();
        this.eventHandlerThread =
            new KafkaThread(threadNamePrefix + "EventHandler", this.eventHandler, false);
        this.eventHandlerThread.start();
        this.timeoutHandler = new TimeoutHandler();
        this.timeoutHandlerThread =
            new KafkaThread(threadNamePrefix + "TimeoutHandler", this.timeoutHandler, false);
        this.timeoutHandlerThread.start();
    }

    @Override
    public <T> CompletableFuture<T> enqueue(boolean append, Throwable clearException,
                                            Long deadlineNs, Event<T> event) {
        lock.lock();
        try {
            EventContext<T> eventContext = new EventContext<>(event, clearException);
            if (closingTimeNs != Long.MAX_VALUE) {
                eventContext.completeWithTimeout();
                return eventContext.future;
            }
            eventHandler.enqueue(append, eventContext);
            if (deadlineNs != null) {
                timeoutHandler.setTimeout(eventContext, deadlineNs);
            }
            return eventContext.future;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(TimeUnit timeUnit, long timeSpan) {
        if (timeSpan <= 0) {
            throw new IllegalArgumentException("shutdown must be called with a " +
                "positive timeout");
        }
        lock.lock();
        try {
            long newClosingTimeNs = Time.SYSTEM.nanoseconds() + timeUnit.toNanos(timeSpan);
            if (closingTimeNs >= newClosingTimeNs) {
                closingTimeNs = newClosingTimeNs;
            }
            timeoutHandler.cond.signal();
            eventHandler.cond.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        shutdown(TimeUnit.DAYS, 100);
        timeoutHandlerThread.join();
        eventHandlerThread.join();
    }
}
