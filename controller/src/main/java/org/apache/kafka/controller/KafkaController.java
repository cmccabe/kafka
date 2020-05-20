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

import kafka.controller.ControllerManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class KafkaController implements Controller {
    private final Logger log;
    private final BackingStore backingStore;
    private final EventQueue controllerQueue;
    private final MetadataState state;
    private final int controllerEpoch;

    KafkaController(LogContext logContext,
                    String threadNamePrefix,
                    BackingStore backingStore,
                    MetadataState state,
                    int controllerEpoch) {
        this.log = logContext.logger(KafkaController.class);
        this.backingStore = backingStore;
        this.controllerQueue = new KafkaEventQueue(logContext, threadNamePrefix);
        this.state = state;
        this.controllerEpoch = controllerEpoch;
    }

    public CompletableFuture<Map<TopicPartition, ControllerManager.PartitionLeaderElectionResult>>
                electLeaders(int timeoutMs, Set<TopicPartition> parts) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void handleBrokerUpdates(List<MetadataState.Broker> changedBrokers, List<Integer> deletedBrokerIds) {

    }

    @Override
    public void handleTopicUpdates(List<MetadataState.Topic> changed, List<String> deleted) {

    }
}
