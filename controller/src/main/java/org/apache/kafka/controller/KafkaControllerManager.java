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
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class KafkaControllerManager implements ControllerManager {
    private final LogContext logContext;
    private final Logger log;
    private final BackingStore backingStore;
    private final EventQueue eventQueue;
    private final KafkaActivationListener activationListener;
    private KafkaController controller;

    @Override
    public CompletableFuture<Void> start(BrokerInfo brokerInfo) {
        return backingStore.start(brokerInfo, this.activationListener);
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

    class KafkaActivationListener implements BackingStore.ActivationListener {
        @Override
        public void activate(KafkaController newController) {
            eventQueue.prepend(new EventQueue.Event<Void>() {
                @Override
                public Void run() {
                    log.info("Activating.");
                    controller = newController;
                    return null;
                }
            });
        }

        @Override
        public void deactivate() {
            eventQueue.prepend(new EventQueue.Event<Void>() {
                @Override
                public Void run() {
                    if (controller != null) {
                        log.info("Deactivating.");
                        controller = null;
                    }
                    return null;
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
        this.activationListener = new KafkaActivationListener();
        this.controller = null;
    }
}
