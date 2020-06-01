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

import org.apache.kafka.common.message.MetadataState;

import java.util.List;

/**
 * Handles events from the BackingStore.
 */
public interface BackingStoreCallbackHandler {
    /**
     * Activate this node as the controller.
     *
     * @param newControllerEpoch    The new controller epoch.
     * @param newState              The current state.
     *
     * @return                  The new controller object.
     */
    void activate(int newControllerEpoch, MetadataState newState);

    /**
     * Deactivate this node as the controller.
     *
     * @param controllerEpoch       The current controller epoch.
     */
    void deactivate(int controllerEpoch);

    /**
     * Handle changes to the brokers in the cluster.
     */
    void handleBrokerUpdates(int controllerEpoch,
                             List<MetadataState.Broker> changedBrokers,
                             List<Integer> deletedBrokerIds);

    /**
     * Handle changes to the topics in the cluster.
     */
    void handleTopicUpdates(int controllerEpoch,
                            TopicDelta topicDelta);
}
