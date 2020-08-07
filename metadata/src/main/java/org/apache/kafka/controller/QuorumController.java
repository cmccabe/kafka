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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.BrokerRegistration;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class QuorumController implements Controller {
    private final Logger log;
    private final Time time;
    private final EventQueue eventQueue;
    private final int nodeId;
    private final MetaLogManager metaLogManager;
    private long currentEpoch;
    private int currentLeaderId;
    private long currentIndex;

    abstract class ControllerEvent<T> implements EventQueue.Event<T> {
        private final long requiredEpoch;
        private final boolean mustBeLeader;

        ControllerEvent(long requiredEpoch, boolean mustBeLeader) {
            this.requiredEpoch = requiredEpoch;
            this.mustBeLeader = mustBeLeader;
        }

        @Override
        public T run() throws Throwable {
            long startNs = time.nanoseconds();
            log.debug("{}: starting", toString());
            Exception exception = null;
            T value = null;
            try {
                if (requiredEpoch != -1 && requiredEpoch != currentEpoch) {
                    throw new QuorumEpochMismatchException();
                }
                if (mustBeLeader && nodeId != currentLeaderId) {
                    throw new QuorumLeaderMismatchException(currentLeaderId);
                }
                value = execute();
            } catch (Exception e) {
                exception = e;
            }
            long endNs = time.nanoseconds();
            long deltaNs = endNs - startNs;
            if (exception != null) {
                log.info("{}: caught exception after {} us",
                    TimeUnit.MICROSECONDS.convert(deltaNs, TimeUnit.NANOSECONDS), exception);
                throw exception;
            }
            log.info("{}: finished after {} us",
                TimeUnit.MICROSECONDS.convert(deltaNs, TimeUnit.NANOSECONDS));
            return value;
        }

        public abstract T execute() throws Throwable;

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    @Override
    public CompletableFuture<Void> handleBrokerHeartbeat(BrokerRegistration registration) {
        return null;
    }

    @Override
    public void beginShutdown() {
    }

    @Override
    public void close() {
    }

    QuorumController(Logger log,
                     Time time,
                     EventQueue eventQueue,
                     int nodeId,
                     MetaLogManager metaLogManager) {
        this.log = log;
        this.time = time;
        this.eventQueue = eventQueue;
        this.nodeId = nodeId;
        this.metaLogManager = metaLogManager;
        this.currentEpoch = -1;
        this.currentLeaderId = -1;
        this.currentIndex = -1;
    }
}
