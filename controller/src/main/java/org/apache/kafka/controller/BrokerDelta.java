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

import org.apache.kafka.common.message.MetadataState.Broker;

import java.util.ArrayList;
import java.util.List;

class BrokerDelta {
    private final List<Broker> changedBrokers;
    private final List<Integer> deletedBrokerIds;

    BrokerDelta() {
        this.changedBrokers = new ArrayList<>();
        this.deletedBrokerIds = new ArrayList<>();
    }

    BrokerDelta(List<Broker> changedBrokers,
                List<Integer> deletedBrokerIds) {
        this.changedBrokers = changedBrokers;
        this.deletedBrokerIds = deletedBrokerIds;
    }

    public List<Broker> changedBrokers() {
        return changedBrokers;
    }

    public List<Integer> deletedBrokerIds() {
        return deletedBrokerIds;
    }
}
