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

package org.apache.kafka.queue;

import org.slf4j.Logger;

import java.util.OptionalLong;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;


/**
 * The EventQueue executes a series of events in its own thread.
 *
 * There are two kinds of events: those that are queued to be run as soon as possible, and those
 * that are scheduled for the future.
 *
 * Events can have deadlines, which are times at which they are completed with a TimeoutException.
 * This is useful for shedding work in the case where the event queue gets too long. Currently,
 * deferred events cannot have deadlines.
 *
 * There are two reasons why Event.handleException might be called. One reason is because the event
 * queue could not run the event because of an exception (like a timeout). Another reason is if the
 * queue ran the event but the run function threw an exception.
 *
 * The default Event.handleException function does not do anything. However, this behavior can be
 * overridden. One simple way of handling exceptions is to log them by subclassing
 * FailureLoggingEvent.
 *
 * The event queue has a shutting down state. When in this state, no new events can be added.
 * Existing events will be run -- unless they are deferred, in which case they will be timed out.
 * At the end of the shutdown process, the cleanup event will always be run.
 *
 * The event queue also has an interrupted state. It enters this state when the thread is sent an
 * InterruptedException or when the thread remains in interrupted state after running a user-supplied
 * event. When in this state, no new events can be added, and all existing events will receive an
 * InterruptedException. However, the queue will not shut down until beginShutdown is called.
 * The cleanup event will be run as usual (it will not receive an InterruptedException unless an
 * external thread sends one).
 *
 * You can always rely on the cleanup event being run
 *
 * beginShutdown is asynchronous and does not clear up the event queue thread. It is necessary to
 * call close() to clean up that thread. The reason beginShutdown is separate from close is that
 * this allows the caller thread that is cleaning up to perform some other actions while waiting
 * for the queue to finish shutting down.
 */
public interface EventQueue extends AutoCloseable {
    interface Event {
        /**
         * Run the event.
         */
        void run() throws Exception;

        /**
         * Handle an exception that was either generated by running the event, or by the
         * event queue's inability to run the event.
         *
         * @param e     The exception.  This will be a TimeoutException if the event hit
         *              its deadline before it could be scheduled.
         *              It will be a RejectedExecutionException if the event could not be
         *              scheduled because the event queue has already been closed.
         *              Otherwise, it will be whatever exception was thrown by run().
         */
        default void handleException(Throwable e) {}
    }

    abstract class FailureLoggingEvent implements Event {
        private final Logger log;

        public FailureLoggingEvent(Logger log) {
            this.log = log;
        }

        @Override
        public void handleException(Throwable e) {
            if (e instanceof RejectedExecutionException) {
                log.info("Not processing {} because the event queue is closed.", this);
            } else {
                log.error("Unexpected error handling {}", this, e);
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    class NoDeadlineFunction implements Function<OptionalLong, OptionalLong> {
        public static final NoDeadlineFunction INSTANCE = new NoDeadlineFunction();

        @Override
        public OptionalLong apply(OptionalLong ignored) {
            return OptionalLong.empty();
        }
    }

    class DeadlineFunction implements Function<OptionalLong, OptionalLong> {
        private final long deadlineNs;

        public DeadlineFunction(long deadlineNs) {
            this.deadlineNs = deadlineNs;
        }

        @Override
        public OptionalLong apply(OptionalLong ignored) {
            return OptionalLong.of(deadlineNs);
        }
    }

    class EarliestDeadlineFunction implements Function<OptionalLong, OptionalLong> {
        private final long newDeadlineNs;

        public EarliestDeadlineFunction(long newDeadlineNs) {
            this.newDeadlineNs = newDeadlineNs;
        }

        @Override
        public OptionalLong apply(OptionalLong prevDeadlineNs) {
            if (!prevDeadlineNs.isPresent()) {
                return OptionalLong.of(newDeadlineNs);
            } else if (prevDeadlineNs.getAsLong() < newDeadlineNs) {
                return prevDeadlineNs;
            } else {
                return OptionalLong.of(newDeadlineNs);
            }
        }
    }

    class VoidEvent implements Event {
        public final static VoidEvent INSTANCE = new VoidEvent();

        @Override
        public void run() throws Exception {
        }
    }

    /**
     * Add an element to the front of the queue.
     *
     * @param event             The mandatory event to prepend.
     */
    default void prepend(Event event) {
        enqueue(EventInsertionType.PREPEND, null, NoDeadlineFunction.INSTANCE, event);
    }

    /**
     * Add an element to the end of the queue.
     *
     * @param event             The event to append.
     */
    default void append(Event event) {
        enqueue(EventInsertionType.APPEND, null, NoDeadlineFunction.INSTANCE, event);
    }

    /**
     * Add an event to the end of the queue.
     *
     * @param deadlineNs        The deadline for starting the event, in monotonic
     *                          nanoseconds.  If the event has not started by this
     *                          deadline, handleException is called with a
     *                          {@link org.apache.kafka.common.errors.TimeoutException},
     *                          and the event is cancelled.
     * @param event             The event to append.
     */
    default void appendWithDeadline(long deadlineNs, Event event) {
        enqueue(EventInsertionType.APPEND, null, new DeadlineFunction(deadlineNs), event);
    }

    /**
     * Schedule an event to be run at a specific time.
     *
     * @param tag                   If this is non-null, the unique tag to use for this
     *                              event.  If an event with this tag already exists, it
     *                              will be cancelled.
     * @param deadlineNsCalculator  A function which takes as an argument the existing
     *                              deadline for the event with this tag (or empty if the
     *                              event has no tag, or if there is none such), and
     *                              produces the deadline to use for this event.
     *                              Once the deadline has arrived, the event will be
     *                              run.  Events whose deadlines are only a few nanoseconds
     *                              apart may be executed in any order.
     * @param event                 The event to schedule.
     */
    default void scheduleDeferred(String tag,
                                  Function<OptionalLong, OptionalLong> deadlineNsCalculator,
                                  Event event) {
        enqueue(EventInsertionType.DEFERRED, tag, deadlineNsCalculator, event);
    }

    /**
     * Cancel a deferred event.
     *
     * @param tag                   The unique tag for the event to be cancelled.  Must be
     *                              non-null.  If the event with the tag has not been
     *                              scheduled, this call will be ignored.
     */
    void cancelDeferred(String tag);

    enum EventInsertionType {
        PREPEND,
        APPEND,
        DEFERRED
    }

    /**
     * Add an event to the queue.
     *
     * @param insertionType         How to insert the event.
     *                              PREPEND means insert the event as the first thing
     *                              to run.  APPEND means insert the event as the last
     *                              thing to run.  DEFERRED means insert the event to
     *                              run after a delay.
     * @param tag                   If this is non-null, the unique tag to use for
     *                              this event.  If an event with this tag already
     *                              exists, it will be cancelled.
     * @param deadlineNsCalculator  If this is non-null, it is a function which takes
     *                              as an argument the existing deadline for the
     *                              event with this tag (or null if the event has no
     *                              tag, or if there is none such), and produces the
     *                              deadline to use for this event (or empty to use
     *                              none.)  Events whose deadlines are only a few
     *                              nanoseconds apart may be executed in any order.
     * @param event                 The event to enqueue.
     */
    void enqueue(EventInsertionType insertionType,
                 String tag,
                 Function<OptionalLong, OptionalLong> deadlineNsCalculator,
                 Event event);

    /**
     * Asynchronously shut down the event queue with no unnecessary delay.
     * @see #beginShutdown(String, Event)
     *
     * @param source                The source of the shutdown.
     */
    default void beginShutdown(String source) {
        beginShutdown(source, new VoidEvent());
    }

    /**
     * Asynchronously shut down the event queue.
     *
     * No new events will be accepted, and the timeout will be initiated
     * for all existing events.
     *
     * @param source        The source of the shutdown.
     * @param cleanupEvent  The mandatory event to invoke after all other events have
     *                      been processed.
     */
    void beginShutdown(String source, Event cleanupEvent);

    /**
     * @return The number of pending and running events. If this is 0, there is no running event and
     * no events queued.
     */
    int size();

    /**
     * @return True if there are no pending or running events.
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * This method is used during unit tests where MockTime is in use.
     * It is used to alert the queue that the mock time has changed.
     */
    default void wakeup() { }

    /**
     * Synchronously close the event queue and wait for any threads to be joined.
     */
    void close() throws InterruptedException;
}
