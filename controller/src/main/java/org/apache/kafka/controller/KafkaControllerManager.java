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
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class KafkaControllerManager implements ControllerManager {
    private final ControllerLogContext logContext;
    private final Logger log;
    private final BackingStore backingStore;
    private final EventQueue mainQueue;
    private final KafkaBackingStoreCallbackHandler backingStoreCallbackHandler;
    private KafkaController controller;
    private boolean started;

    abstract class AbstractControllerManagerEvent<T> extends AbstractEvent<T> {
        public AbstractControllerManagerEvent(Optional<Integer> requiredControllerEpoch) {
            super(log, requiredControllerEpoch);
        }

        @Override
        public Optional<Integer> currentControllerEpoch() {
            if (controller == null) {
                return Optional.empty();
            }
            return Optional.of(controller.controllerEpoch());
        }

        @Override
        public Throwable handleException(Throwable e) throws Throwable {
            return e;
        }
    }

    class KafkaBackingStoreCallbackHandler implements BackingStoreCallbackHandler {
        @Override
        public void activate(int newControllerEpoch, MetadataState newState) {
            mainQueue.append(new ActivateEvent(newControllerEpoch, newState));
        }

        @Override
        public void deactivate(int controllerEpoch) {
            mainQueue.prepend(new DeactivateEvent(controllerEpoch));
        }

        @Override
        public void handleBrokerUpdates(int controllerEpoch,
                                        List<MetadataState.Broker> changedBrokers,
                                        List<Integer> deletedBrokerIds) {
            mainQueue.append(new HandleBrokerUpdates(controllerEpoch, changedBrokers,
                deletedBrokerIds));
        }

        @Override
        public void handleTopicUpdates(int controllerEpoch, TopicDelta topicDelta) {
            mainQueue.append(new HandleTopicUpdates(controllerEpoch, topicDelta));
        }
    }

    class StartEvent extends AbstractControllerManagerEvent<Void> {
        private final BrokerInfo newBrokerInfo;

        public StartEvent(BrokerInfo newBrokerInfo) {
            super(Optional.empty());
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void execute() throws Throwable {
            if (started) {
                throw new RuntimeException("Attempting to Start a KafkaControllerManager " +
                    "which has already been started.");
            }
            backingStore.start(newBrokerInfo, backingStoreCallbackHandler).get();
            started = true;
            return null;
        }
    }

    class StopEvent extends AbstractControllerManagerEvent<Void> {
        public StopEvent() {
            super(Optional.empty());
        }

        @Override
        public Void execute() throws Throwable {
            if (!started) {
                return null;
            }
            controller = null;
            return null;
        }
    }

    class ActivateEvent extends AbstractControllerManagerEvent<Void> {
        private final int newControllerEpoch;
        private final MetadataState newState;

        public ActivateEvent(int newControllerEpoch, MetadataState newState) {
            super(Optional.empty());
            this.newControllerEpoch = newControllerEpoch;
            this.newState = newState;
        }

        @Override
        public Void execute() throws Throwable {
            controller = new KafkaController(logContext,
                newControllerEpoch,
                backingStore,
                mainQueue,
                newState);
            return null;
        }
    }

    class DeactivateEvent extends AbstractControllerManagerEvent<Void> {
        public DeactivateEvent(int controllerEpoch) {
            super(Optional.of(controllerEpoch));
        }

        @Override
        public Void execute() throws Throwable {
            controller = null;
            return null;
        }
    }

    class HandleBrokerUpdates extends AbstractControllerManagerEvent<Void> {
        private final List<MetadataState.Broker> changedBrokers;
        private final List<Integer> deletedBrokerIds;

        public HandleBrokerUpdates(int controllerEpoch,
                List<MetadataState.Broker> changedBrokers,
                List<Integer> deletedBrokerIds) {
            super(Optional.of(controllerEpoch));
            this.changedBrokers = changedBrokers;
            this.deletedBrokerIds = deletedBrokerIds;
        }

        @Override
        public Void execute() throws Throwable {
            return null;
        }
    }

    class HandleTopicUpdates extends AbstractControllerManagerEvent<Void> {
        private final TopicDelta topicDelta;

        public HandleTopicUpdates(int controllerEpoch, TopicDelta topicDelta) {
            super(Optional.of(controllerEpoch));
            this.topicDelta = topicDelta;
        }

        @Override
        public Void execute() throws Throwable {
            return null;
        }
    }

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        ControllerLogContext logContext = 
            new ControllerLogContext(factory.threadNamePrefix(),
                new LogContext(String.format("[Node %d]", factory.nodeId())));
        boolean success = false;
        ZkBackingStore zkBackingStore = null;
        KafkaEventQueue mainQueue = null;
        KafkaControllerManager controllerManager = null;
        try {
            zkBackingStore = ZkBackingStore.create(logContext,
                factory.nodeId(),
                factory.zkClient());
            mainQueue = new KafkaEventQueue(new LogContext(
                logContext.logContext().logPrefix() + " [mainQueue] "),
                logContext.threadNamePrefix());
            controllerManager = new KafkaControllerManager(logContext,
                zkBackingStore, mainQueue);
            success = true;
        } finally {
            if (!success) {
                Utils.closeQuietly(zkBackingStore, "zkBackingStore");
                Utils.closeQuietly(mainQueue, "mainQueue");
            }
        }
        return controllerManager;
    }

    KafkaControllerManager(ControllerLogContext logContext,
                           BackingStore backingStore,
                           EventQueue mainQueue) {
        this.logContext = logContext;
        this.log = logContext.createLogger(KafkaControllerManager.class);
        this.backingStore = backingStore;
        this.mainQueue = mainQueue;
        this.backingStoreCallbackHandler = new KafkaBackingStoreCallbackHandler();
        this.controller = null;
        this.started = false;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo) {
        return mainQueue.append(new StartEvent(newBrokerInfo));
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
    public CompletableFuture<Set<TopicPartition>> controlledShutdown(int brokerId,
                                                                     int brokerEpoch) {
        return null;
    }

    @Override
    public void shutdown() {
        log.debug("Shutting down.");
        mainQueue.shutdown(new StopEvent());
        backingStore.shutdown();
    }

    @Override
    public void close() throws Exception {
        log.debug("Initiating close.");
        shutdown();
        mainQueue.close();
        backingStore.close();
        log.debug("Close complete.");
    }
}
