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

import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a set of changes to the topics in the cluster.  There are several possible
 * changes encoded here: removing a topic, adding a topic, removing a partition, adding a
 * partition, removing a replica and adding a replica.
 * Changes to the in-sync replica set show up as changed partitions.
 */
class TopicDelta {
    static class ReplicaChange {
        final List<Integer> replicas;
        final List<Integer> addingReplicas;
        final List<Integer> removingReplicas;

        ReplicaChange(List<Integer> replicas,
                      List<Integer> addingReplicas,
                      List<Integer> removingReplicas) {
            this.replicas = replicas;
            this.addingReplicas = addingReplicas;
            this.removingReplicas = removingReplicas;
        }

        boolean matches(MetadataState.Partition existingPart) {
            if (!replicas.equals(existingPart.replicas())) return false;
            if (!addingReplicas.equals(existingPart.addingReplicas())) return false;
            if (!removingReplicas.equals(existingPart.removingReplicas())) return false;
            return true;
        }

        MetadataState.Partition toPartition(int partitionId) {
            MetadataState.Partition part = new MetadataState.Partition().setId(partitionId);
            apply(part);
            return part;
        }

        void apply(MetadataState.Partition part) {
            part.setReplicas(new ArrayList<>(replicas)).
                setAddingReplicas(new ArrayList<>(addingReplicas)).
                setRemovingReplicas(new ArrayList<>(removingReplicas));
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, addingReplicas, removingReplicas);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ReplicaChange)) return false;
            ReplicaChange other = (ReplicaChange) o;
            return replicas.equals(other.replicas) &&
                addingReplicas.equals(other.addingReplicas) &&
                removingReplicas.equals(other.removingReplicas);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("ReplicaChange(");
            bld.append("replicas = ").append(Utils.join(replicas, ", "));
            bld.append(", addingReplicas = ").append(Utils.join(addingReplicas, ", "));
            bld.append(", removingReplicas = ").append(Utils.join(removingReplicas, ", "));
            bld.append(")");
            return bld.toString();
        }
    }

    static class IsrChange {
        final int leader;
        final int leaderEpoch;
        final List<Integer> isr;
        final int controllerEpoch;
        final int zkVersion;

        static IsrChange fromLeaderIsrAndControllerEpoch(LeaderIsrAndControllerEpoch info) {
            return new IsrChange(info.leaderAndIsr().leader(),
                info.leaderAndIsr().leaderEpoch(),
                CoreUtils.asJava(info.leaderAndIsr().isr()),
                info.controllerEpoch(),
                info.leaderAndIsr().zkVersion());
        }

        IsrChange(int leader, int leaderEpoch, List<Integer> isr, int controllerEpoch,
                  int zkVersion) {
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.isr = isr;
            this.controllerEpoch = controllerEpoch;
            this.zkVersion = zkVersion;
        }

        boolean matches(MetadataState.Partition part) {
            if (leader != part.leader()) return false;
            if (leaderEpoch != part.leaderEpoch()) return false;
            if (!isr.equals(part.isr())) return false;
            if (controllerEpoch != part.controllerEpochOfLastIsrUpdate()) return false;
            if (zkVersion != part.zkVersionOfLastIsrUpdate()) return false;
            return true;
        }

        MetadataState.Partition toPartition(int partitionId) {
            MetadataState.Partition part = new MetadataState.Partition().setId(partitionId);
            apply(part);
            return part;
        }

        void apply(MetadataState.Partition part) {
            part.setLeader(leader).
                setLeaderEpoch(leaderEpoch).
                setIsr(new ArrayList<>(isr)).
                setControllerEpochOfLastIsrUpdate(controllerEpoch).
                setZkVersionOfLastIsrUpdate(zkVersion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leader, leaderEpoch, isr, controllerEpoch, zkVersion);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof IsrChange)) return false;
            IsrChange other = (IsrChange) o;
            return leader == other.leader &&
                leaderEpoch == other.leaderEpoch &&
                isr.equals(other.isr) &&
                controllerEpoch == other.controllerEpoch &&
                zkVersion == other.zkVersion;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("IsrChange(");
            bld.append("leader = ").append(leader);
            bld.append(", leaderEpoch = ").append(leaderEpoch);
            bld.append(", isr = ").append(Utils.join(isr, ", "));
            bld.append(", controllerEpoch = ").append(controllerEpoch);
            bld.append(", zkVersion = ").append(zkVersion);
            bld.append(")");
            return bld.toString();
        }
    }

    final List<MetadataState.Topic> addedTopics = new ArrayList<>();

    final List<String> removedTopics = new ArrayList<>();

    final Map<TopicPartition, MetadataState.Partition> addedParts = new HashMap<>();

    final List<TopicPartition> removedParts = new ArrayList<>();

    final Map<TopicPartition, ReplicaChange> replicaChanges = new HashMap<>();

    final Map<TopicPartition, IsrChange> isrChanges = new HashMap<>();

    static TopicDelta fromSingleTopicRemoval(String topic) {
        TopicDelta topicDelta = new TopicDelta();
        topicDelta.removedTopics.add(topic);
        return topicDelta;
    }

    static TopicDelta fromUpdatedTopicReplicas(MetadataState.TopicCollection existingTopics,
                                               MetadataState.Topic updatedTopic) {
        TopicDelta topicDelta = new TopicDelta();
        MetadataState.TopicCollection updatedTopics = new MetadataState.TopicCollection();
        updatedTopics.mustAdd(updatedTopic.duplicate());
        topicDelta.calculateReplicaUpdates(existingTopics, updatedTopics);
        return topicDelta;
    }

    static TopicDelta fromUpdatedTopicReplicas(MetadataState.TopicCollection existingTopics,
                                               MetadataState.TopicCollection updatedTopics) {
        TopicDelta topicDelta = new TopicDelta();
        for (MetadataState.Topic existingTopic : existingTopics) {
            MetadataState.Topic updatedTopic = updatedTopics.find(existingTopic);
            if (updatedTopic == null) {
                topicDelta.removedTopics.add(existingTopic.name());
            }
        }
        topicDelta.calculateReplicaUpdates(existingTopics, updatedTopics);
        return topicDelta;
    }

    static TopicDelta fromIsrUpdates(MetadataState.TopicCollection existingTopics,
                Map<TopicPartition, LeaderIsrAndControllerEpoch> updates) {
        Map<String, Map<Integer, LeaderIsrAndControllerEpoch>> groupedUpdates = new HashMap<>();
        for (Map.Entry<TopicPartition, LeaderIsrAndControllerEpoch> entry :
                updates.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            groupedUpdates.put(topicPartition.topic(),
                groupedUpdates.computeIfAbsent(topicPartition.topic(),
                    __ -> new HashMap<>())).put(topicPartition.partition(), entry.getValue());
        }
        TopicDelta delta = new TopicDelta();
        for (Map.Entry<String, Map<Integer, LeaderIsrAndControllerEpoch>> entry :
                groupedUpdates.entrySet()) {
            String topicName = entry.getKey();
            Map<Integer, LeaderIsrAndControllerEpoch> partInfo = entry.getValue();
            MetadataState.Topic existingTopic = existingTopics.find(topicName);
            if (existingTopic == null) {
                MetadataState.Topic addedTopic =
                    new MetadataState.Topic().setName(topicName);
                for (Map.Entry<Integer, LeaderIsrAndControllerEpoch> partEntry :
                        partInfo.entrySet()) {
                    IsrChange isrChange = IsrChange.
                        fromLeaderIsrAndControllerEpoch(partEntry.getValue());
                    MetadataState.Partition part =
                        isrChange.toPartition(partEntry.getKey());
                    addedTopic.partitions().mustAdd(part);
                }
                delta.addedTopics.add(addedTopic);
            } else {
                for (Map.Entry<Integer, LeaderIsrAndControllerEpoch> partEntry :
                        partInfo.entrySet()) {
                    int partId = partEntry.getKey();
                    IsrChange isrChange = IsrChange.
                        fromLeaderIsrAndControllerEpoch(partEntry.getValue());
                    MetadataState.Partition part = existingTopic.partitions().find(partId);
                    TopicPartition topicPart = new TopicPartition(topicName, partId);
                    if (part == null) {
                        part = isrChange.toPartition(partId);
                        delta.addedParts.put(topicPart, part);
                    } else if (!isrChange.matches(part)) {
                        delta.isrChanges.put(topicPart, isrChange);
                    }
                }
            }
        }
        return delta;
    }

    private void calculateReplicaUpdates(MetadataState.TopicCollection existingTopics,
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
                TopicPartition topicPart =
                    new TopicPartition(existingTopic.name(), existingPart.id());
                MetadataState.Partition updatedPart =
                    updatedTopic.partitions().find(existingPart);
                if (updatedPart == null) {
                    removedParts.add(topicPart);
                    continue;
                }
                ReplicaChange change = new ReplicaChange(updatedPart.replicas(),
                    updatedPart.addingReplicas(), updatedPart.removingReplicas());
                if (!change.matches(existingPart)) {
                    replicaChanges.put(topicPart, change);
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
        for (Map.Entry<TopicPartition, ReplicaChange> entry : replicaChanges.entrySet()) {
            TopicPartition topicPart = entry.getKey();
            ReplicaChange change = entry.getValue();
            MetadataState.Topic topic = topics.getOrCreate(topicPart.topic());
            MetadataState.Partition part =
                topic.partitions().getOrCreate(topicPart.partition());
            change.apply(part);
        }
        for (Map.Entry<TopicPartition, IsrChange> entry : isrChanges.entrySet()) {
            TopicPartition topicPart = entry.getKey();
            IsrChange change = entry.getValue();
            MetadataState.Topic topic = topics.getOrCreate(topicPart.topic());
            MetadataState.Partition part =
                topic.partitions().getOrCreate(topicPart.partition());
            change.apply(part);
        }
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("TopicDelta(");
        bld.append("addedTopics=").append(Utils.join(addedTopics, ","));
        bld.append(", removedTopics=").append(Utils.join(removedTopics, ","));
        bld.append(", addedParts=").
            append(Utils.mkString(addedParts, "{", "}", "=", ","));
        bld.append(", removedParts=").append(Utils.join(removedParts, ","));
        bld.append(", replicaChanges=").
            append(Utils.mkString(replicaChanges, "{", "}", "=", ","));
        bld.append(", isrChanges=").
            append(Utils.mkString(isrChanges, "{", "}", "=", ","));
        bld.append(")");
        return bld.toString();
    }
}
