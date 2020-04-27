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
import org.apache.kafka.common.message.MetadataStateData;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class KafkaControllerManager implements ControllerManager {
    private final LogContext logContext;
    private final Logger log;
    private final BackingStore backingStore;
    private final EventQueue eventQueue;
    private final KafkaChangeListener changeListener;
    private MetadataStateData state;

    @Override
    public CompletableFuture<Void> start(BrokerInfo brokerInfo) {
        return backingStore.start(brokerInfo, this.changeListener);
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

    class KafkaChangeListener implements BackingStore.ChangeListener {
        @Override
        public void activate(MetadataStateData newState) {
            eventQueue.prepend(new EventQueue.Event<Void>() {
                @Override
                public Void run() {
                    log.info("Activating.");
                    state = newState;
                    return null;
                }
            });
        }

        @Override
        public void deactivate() {
            eventQueue.prepend(new EventQueue.Event<Void>() {
                @Override
                public Void run() {
                    if (state != null) {
                        log.info("Deactivating.");
                        state = null;
                    }
                    return null;
                }
            });
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
    }

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        LogContext logContext =
            new LogContext(String.format("[Node %d]", factory.nodeId()));
        return new KafkaControllerManager(logContext, ZkBackingStore.
            create(factory.nodeId(), factory.threadNamePrefix(), factory.zkClient()),
            new KafkaEventQueue(logContext, factory.threadNamePrefix()));
    }

    KafkaControllerManager(LogContext logContext,
                           BackingStore backingStore,
                           EventQueue eventQueue) {
        this.logContext = logContext;
        this.log = logContext.logger(KafkaControllerManager.class);
        this.backingStore = backingStore;
        this.eventQueue = eventQueue;
        this.changeListener = new KafkaChangeListener();
        this.state = null;
    }
}
