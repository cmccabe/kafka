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

import kafka.controller.ControllerManagerFactory;
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.message.MetadataStateData;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public final class KafkaControllerManager implements ControllerManager {
    private final LogContext logContext;
    private final Logger log;
    private final AtomicReference<Throwable> lastUnexpectedError;
    private final BackingStore backingStore;
    private final EventQueue eventQueue;
    private final KafkaChangeListener changeListener;
    private MetadataStateData state;
    private boolean started;

    class KafkaChangeListener implements BackingStore.ChangeListener {
        @Override
        public void activate(MetadataStateData newState) {
            eventQueue.append(new ActivateEvent(newState));
        }

        @Override
        public void deactivate() {
            eventQueue.append(new DeactivateEvent());
        }

        @Override
        public void handleBrokerUpdates(List<MetadataStateData.Broker> changedBrokers,
                                        List<Integer> deletedBrokerIds) {
            eventQueue.append(new EventQueue.Event<Void>() {
                @Override
                public Void run() {
//                    if (state == null) return null;
//                    for (MetadataStateData.Broker newBroker : newBrokers) {
//                        state.brokers().mustAdd(newBroker);
//                    }
//                    for (MetadataStateData.Broker bouncedBroker : bouncedBrokers) {
//                        state.brokers().remove(bouncedBroker);
//                        state.brokers().mustAdd(bouncedBroker);
//                    }
//                    for (int deletedBrokerId : deletedBrokerIds) {
//                        state.brokers().remove(new MetadataStateData.Broker().
//                            setBrokerId(deletedBrokerId));
//                    }
//                    return null;
                    throw new RuntimeException("not implemented");
                }
            });
        }

        @Override
        public void handleTopicUpdates(List<MetadataStateData.Topic> changed, List<String> deleted) {

        }
    }

    abstract class AbstractEvent<T> implements EventQueue.Event<T> {
        final String name;

        AbstractEvent() {
            this.name = getClass().getSimpleName();
        }

        @Override
        final public T run() throws Throwable {
            long startNs = Time.SYSTEM.nanoseconds();
            log.info("{}: starting", name);
            try {
                T value = execute();
                log.info("{}: finished after {} ms",
                    name, executionTimeToString(startNs));
                return value;
            } catch (Throwable e) {
                if (e instanceof ExecutionException && e.getCause() != null) {
                    e = e.getCause();
                }
                if (e instanceof ApiException) {
                    log.info("{}: caught {} after {} ms.", name,
                        e.getClass().getSimpleName(), executionTimeToString(startNs));
                    throw e;
                } else {
                    log.error("{}: finished after {} ms with unexpected error", name,
                        executionTimeToString(startNs), e);
                    lastUnexpectedError.set(e);
//                    if (state != null) {
//                        backingStore.resign();
//                    }
                }
            }
            return null;
        }

        final private String executionTimeToString(long startNs) {
            long endNs = Time.SYSTEM.nanoseconds();
            return ControllerUtils.nanosToFractionalMillis(endNs - startNs);
        }

        public abstract T execute() throws Throwable;
    }

    /**
     * Check that the KafkaControllerManager has been started.  This is a sanity check
     * that the developer remembered to call start().
     */
    private void checkIsStarted() {
        if (!started) {
            throw new RuntimeException("The KafkaControllerManager has not been started.");
        }
    }

    /**
     * Check that the KafkaControllerManager has been started and is currently active.
     */
    private void checkIsStartedAndActive() {
        checkIsStarted();
        if (state == null) {
            throw new NotControllerException("This node is not the active controller.");
        }
    }

    /**
     Initialize the KafkaControllerManager.
     */
    class StartEvent extends AbstractEvent<Void> {
        private final BrokerInfo newBrokerInfo;

        StartEvent(BrokerInfo newBrokerInfo) {
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void execute() throws Throwable {
            if (started) {
                throw new RuntimeException("Attempting to Start a KafkaControllerManager " +
                    "which has already been started.");
            }
            backingStore.start(newBrokerInfo, changeListener).get();
            started = true;
            return null;
        }
    }

    /**
     * Activate the KafkaControllerManager.
     */
    class ActivateEvent extends AbstractEvent<Void> {
        private final MetadataStateData newState;

        ActivateEvent(MetadataStateData newState) {
            this.newState = newState;
        }

        @Override
        public Void execute() throws Throwable {
            checkIsStarted();
            state = newState;
            return null;
        }
    }

    /**
     * Deactivate the KafkaControllerManager.
     */
    class DeactivateEvent extends AbstractEvent<Void> {
        @Override
        public Void execute() throws Throwable {
            checkIsStartedAndActive();
            state = null;
            return null;
        }
    }

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        LogContext logContext =
            new LogContext(String.format("[Node %d]", factory.nodeId()));
        AtomicReference<Throwable> lastUnexpectedError = new AtomicReference<>(null);
        ZkBackingStore zkBackingStore = ZkBackingStore.create(lastUnexpectedError,
                factory.nodeId(),
                factory.threadNamePrefix(),
                factory.zkClient());
        return new KafkaControllerManager(logContext,
                lastUnexpectedError,
                zkBackingStore,
                new KafkaEventQueue(logContext, factory.threadNamePrefix()));
    }

    KafkaControllerManager(LogContext logContext,
                           AtomicReference<Throwable> lastUnexpectedError,
                           BackingStore backingStore,
                           EventQueue eventQueue) {
        this.logContext = logContext;
        this.log = logContext.logger(KafkaControllerManager.class);
        this.lastUnexpectedError = lastUnexpectedError;
        this.backingStore = backingStore;
        this.eventQueue = eventQueue;
        this.changeListener = new KafkaChangeListener();
        this.state = null;
        this.started = false;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo) {
        return eventQueue.append(new StartEvent(newBrokerInfo));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return backingStore.updateBrokerInfo(newBrokerInfo);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
            electLeaders(int timeoutMs, Set<TopicPartition> parts) {
        return null;
    }

    @Override
    public CompletableFuture<Set<TopicPartition>> controlledShutdown(int brokerId, int brokerEpoch) {
        return null;
    }
}
