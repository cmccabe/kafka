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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.message.MetadataState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a set of changes to the topics in the cluster.  There are several possible
 * changes encoded here: removing a topic, adding a topic, removing a partition, adding a
 * partition, removing a replica and adding a replica.
 * We do not encode changes to topic configurations or in-sync replica sets here.
 */
class TopicDelta {
    final List<MetadataState.Topic> addedTopics = new ArrayList<>();
    final List<String> removedTopics = new ArrayList<>();
    final Map<TopicPartition, MetadataState.Partition> addedParts = new HashMap<>();
    final List<TopicPartition> removedParts = new ArrayList<>();
    final Map<TopicPartitionReplica, MetadataState.Replica> addedReplicas = new HashMap<>();
    final List<TopicPartitionReplica> removedReplicas = new ArrayList<>();

    static TopicDelta fromSingleTopicRemoval(String topic) {
        TopicDelta topicDelta = new TopicDelta();
        topicDelta.removedTopics.add(topic);
        return topicDelta;
    }

    static TopicDelta fromUpdatedTopic(MetadataState.TopicCollection existingTopics,
                                       MetadataState.Topic updatedTopic) {
        TopicDelta topicDelta = new TopicDelta();
        MetadataState.TopicCollection updatedTopics = new MetadataState.TopicCollection();
        updatedTopics.mustAdd(updatedTopic);
        topicDelta.calculateUpdates(existingTopics, updatedTopics);
        return topicDelta;
    }

    static TopicDelta fromUpdatedTopics(MetadataState.TopicCollection existingTopics,
                                        MetadataState.TopicCollection updatedTopics) {
        TopicDelta topicDelta = new TopicDelta();
        for (MetadataState.Topic existingTopic : existingTopics) {
            MetadataState.Topic updatedTopic = updatedTopics.find(existingTopic);
            if (updatedTopic == null) {
                topicDelta.removedTopics.add(existingTopic.name());
            }
        }
        topicDelta.calculateUpdates(existingTopics, updatedTopics);
        return topicDelta;
    }

    private void calculateUpdates(MetadataState.TopicCollection existingTopics,
                                  MetadataState.TopicCollection updatedTopics) {
        for (MetadataState.Topic updatedTopic : updatedTopics) {
            MetadataState.Topic existingTopic = existingTopics.find(updatedTopic);
            if (existingTopic == null) {
                addedTopics.add(updatedTopic.duplicate());
                continue;
            }
            for (MetadataState.Partition updatedPart : updatedTopic.partitions()) {
                MetadataState.Partition existingPart =
                    existingTopic.partitions().find(updatedPart);
                if (existingPart == null) {
                    addedParts.put(new TopicPartition(updatedTopic.name(), updatedPart.id()),
                        updatedPart.duplicate());
                }
            }
            for (MetadataState.Partition existingPart : existingTopic.partitions()) {
                MetadataState.Partition updatedPart =
                    existingTopic.partitions().find(existingPart);
                if (updatedPart == null) {
                    removedParts.add(new TopicPartition(updatedTopic.name(), updatedPart.id()));
                    continue;
                }
                for (MetadataState.Replica existingReplica : existingPart.replicas()) {
                    MetadataState.Replica newReplica =
                        updatedPart.replicas().find(existingReplica);
                    if (newReplica == null) {
                        removedReplicas.add(new TopicPartitionReplica(existingTopic.name(),
                            existingPart.id(), existingReplica.id()));
                        continue;
                    }
                }
                for (MetadataState.Replica newReplica : updatedPart.replicas()) {
                    MetadataState.Replica existingReplica =
                        existingPart.replicas().find(newReplica);
                    if (existingReplica == null) {
                        addedReplicas.put(new TopicPartitionReplica(updatedTopic.name(),
                            updatedPart.id(), newReplica.id()), newReplica.duplicate());
                        continue;
                    }
                }
            }
        }
    }

    void apply(MetadataState.TopicCollection topics) {
        for (MetadataState.Topic addedTopic : addedTopics) {
            topics.add(addedTopic.duplicate());
        }
        for (String removedTopic : removedTopics) {
            topics.remove(new MetadataState.Topic().setName(removedTopic));
        }
        for (Map.Entry<TopicPartition, MetadataState.Partition> entry : addedParts.entrySet()) {
            TopicPartition partition = entry.getKey();
            topics.find(partition.topic()).partitions().
                    mustAdd(entry.getValue().duplicate());
        }
        for (TopicPartition removedPartition : removedParts) {
            topics.find(removedPartition.topic()).partitions().remove(
                new MetadataState.Partition().setId(removedPartition.partition()));
        }
        for (Map.Entry<TopicPartitionReplica, MetadataState.Replica> entry : addedReplicas.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            MetadataState.Replica addedReplica = entry.getValue();
            topics.find(replica.topic()).partitions().find(
                replica.partition()).replicas().mustAdd(addedReplica);
        }
        for (TopicPartitionReplica replica : removedReplicas) {
            topics.find(replica.topic()).partitions().find(replica.partition()).
                replicas().remove(new MetadataState.Replica().setId(replica.brokerId()));
        }
    }
}
