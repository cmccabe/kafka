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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public interface EventQueue extends AutoCloseable {
    interface Event<T> {
        T run();
    }

    class VoidEvent implements Event<Void> {
        public final static VoidEvent INSTANCE = new VoidEvent();

        @Override
        public Void run() {
            return null;
        }
    }

    /**
     * Add an element to the end of the queue.
     *
     * @param event             The event to append.
     *
     * @return                  A future which is completed with an exception or
     *                          with the result of Event#run.
     */
    default <T> CompletableFuture<T> prepend(Event<T> event) {
        return enqueue(false, null, null, event);
    }

    /**
     * Add an element to the end of the queue.
     *
     * @param event             The event to append.
     *
     * @return                  A future which is completed with an exception or
     *                          with the result of Event#run.
     */
    default <T> CompletableFuture<T> append(Event<T> event) {
        return enqueue(true, null, null, event);
    }

    /**
     * Clear all existing elements, then append a new element to the end of the queue.
     *
     * @param t                 The exception to deliver to all existing events.
     * @param event             The event to add after all existing events are cleared.
     *
     * @return                  A future which is completed with an exception or
     *                          with the result of Event#run.
     */
    default <T> CompletableFuture<T> clearAndEnqueue(Throwable t, Event<T> event) {
        return enqueue(false, t, null, event);
    }

    /**
     * Enqueue an event to be run in FIFO order.
     *
     * @param deadlineNs        The time in monotonic nanoseconds after which the future
     *                          is completed with a
     *                          @{org.apache.kafka.common.errors.TimeoutException},
     *                          and the event is cancelled, or null if there is no timeout.
     * @param event             The event to append.
     *
     * @return                  A future which is completed with an exception or
     *                          with the result of Event#run.  If there was a
     *                          timeout, the event will not be run.
     */
    default <T> CompletableFuture<T> appendWithDeadline(long deadlineNs, Event<T> event) {
        return enqueue(true, null, deadlineNs, event);
    }

    /**
     * Enqueue an event to be run in FIFO order.
     *
     * @param append            True if the element should be appended to the end of
     *                          the queue.  False if the element should be inserted to
     *                          the beginning.
     * @param clearException    null, or an exception to deliver to all queued elements
     *                          after this one.
     * @param deadlineNs        The time in monotonic nanoseconds after which the future
     *                          is completed with a
     *                          @{org.apache.kafka.common.errors.TimeoutException},
     *                          and the event is cancelled, or null if there is no timeout.
     * @param event             The event to enqueue.
     *
     * @return                  A future which is completed with an exception or
     *                          with the result of Event#run.  If there was a
     *                          timeout, the event will not be run.
     */
    <T> CompletableFuture<T> enqueue(boolean append, Throwable clearException,
                                     Long deadlineNs, Event<T> event);

    /**
     * Asynchronously shut down the event queue.
     *
     * No new events will be accepted, and the timeout will be initiated
     * for all existing events.
     *
     * @param timeUnit      The time unit to use for the timeout.
     * @param timeSpan      The amount of time to use for the timeout.
     *                      Once the timeout elapses, any remaining queued
     *                      events will get a
     *                      @{org.apache.kafka.common.errors.ShutdownException}.
     */
    void shutdown(TimeUnit timeUnit, long timeSpan);

    /**
     * Synchronously close the event queue and wait for any threads to be joined.
     */
    void close() throws InterruptedException, ExecutionException;
}
