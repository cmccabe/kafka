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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * Represents changes to the cluster in the metadata image.
 */
public final class ClusterDelta {
    private final ClusterImage image;
    private final HashMap<Integer, Optional<NodeImage>> changedBrokers = new HashMap<>();

    public ClusterDelta(ClusterImage image) {
        this.image = image;
    }

    public HashMap<Integer, Optional<NodeImage>> changedBrokers() {
        return changedBrokers;
    }

    public NodeImage broker(int nodeId) {
        Optional<NodeImage> result = changedBrokers.get(nodeId);
        if (result != null) {
            return result.orElse(null);
        }
        return image.broker(nodeId);
    }

    public void replay(RegisterBrokerRecord record) {
        NodeImage nodeImage = new NodeImage(record);
        changedBrokers.put(nodeImage.nodeId(), Optional.of(nodeImage));
    }

    public void replay(UnregisterBrokerRecord record) {
        changedBrokers.put(record.brokerId(), Optional.empty());
    }

    public void replay(FenceBrokerRecord record) {
        NodeImage nodeImage = broker(record.id());
        if (nodeImage == null) {
            throw new RuntimeException("Tried to fence broker " + record.id() +
                ", but that broker was not registered.");
        }
        changedBrokers.put(record.id(), Optional.of(new NodeImage(nodeImage, true)));
    }

    public void replay(UnfenceBrokerRecord record) {
        NodeImage nodeImage = broker(record.id());
        if (nodeImage == null) {
            throw new RuntimeException("Tried to unfence broker " + record.id() +
                ", but that broker was not registered.");
        }
        changedBrokers.put(record.id(), Optional.of(new NodeImage(nodeImage, false)));
    }

    public ClusterImage apply() {
        Map<Integer, NodeImage> newBrokers = new HashMap<>(image.brokers().size());
        for (Entry<Integer, NodeImage> entry : image.brokers().entrySet()) {
            int nodeId = entry.getKey();
            Optional<NodeImage> change = changedBrokers.get(nodeId);
            if (change == null) {
                newBrokers.put(nodeId, entry.getValue());
            } else if (change.isPresent()) {
                newBrokers.put(nodeId, change.get());
            }
        }
        for (Entry<Integer, Optional<NodeImage>> entry : changedBrokers.entrySet()) {
            int nodeId = entry.getKey();
            Optional<NodeImage> nodeImage = entry.getValue();
            if (!newBrokers.containsKey(nodeId)) {
                if (nodeImage.isPresent()) {
                    newBrokers.put(nodeId, nodeImage.get());
                }
            }
        }
        return new ClusterImage(newBrokers);
    }
}
