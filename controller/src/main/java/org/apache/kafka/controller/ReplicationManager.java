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
import org.apache.kafka.common.message.MetadataState;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The ReplicaManager manages the partition replication.
 *
 * There are two kinds of partition state that it manages.  The first is hard state, which
 * is ultimately stored in the backing store.  This includes things like partition epochs,
 * ISR membership, and so on.  The second kind is soft state which exists only in memory.
 *
 * Its methods are intended to be called from the KafkaControllerManager's main queue
 * thread.  Therefore, there is no internal synchronization.
 */
public final class ReplicationManager {
    enum ReplicaState {
        /**
         * The replica is present on the broker, but not in sync.
         */
        PRESENT,

        /**
         * The replica is present on the broker and in sync.
         */
        IN_SYNC,
    }

    /**
     * Maps broker ids to maps of replica state to partition sets.
     * This allows us to quickly iterate over all the partitions in a given state on a
     * given broker.
     */
    private final Map<Integer, Map<ReplicaState, Set<TopicPartition>>>
        replicaStates = new HashMap<>();

    /**
     * The current metadata state.  Only changes which have been written to the backing
     * store are included here.
     */
    private final MetadataState state;

    ReplicationManager(MetadataState state) {
        this.state = state;
    }

    /**
     * Get a reference to the current metadata state.
     */
    public MetadataState state() {
        return state;
    }

    /**
     * Checks if a given broker has any in-sync replicas.
     *
     * @param brokerId  The broker ID to check.
     * @return          True only if the broker has any in-sync replicas.
     */
    boolean hasInSyncReplicas(int brokerId) {
        Map<ReplicaState, Set<TopicPartition>> statesToReplicas = replicaStates.get(brokerId);
        if (statesToReplicas == null) return false;
        Set<TopicPartition> partitions = statesToReplicas.get(ReplicaState.IN_SYNC);
        if (partitions == null) return false;
        return !partitions.isEmpty();
    }

    /**
     * Handle changes to the set of brokers.
     *
     * @param delta     The delta to apply.
     */
    public void handleBrokerUpdates(BrokerDelta delta, BackingStore backingStore) {
        IsrDelta isrDelta = new IsrDelta();
        for (int brokerId : delta.deletedBrokerIds()) {
            if (delta.changedBrokers().find(brokerId) != null) {
                // If the broker appears in both changed brokers and deleted broker IDs,
                // it was bounced, and not removed.  So we don't need to take any action
                // regarding its in-sync replicas.
                continue;
            }
            // For brokers that were removed, we want to remove their replicas from the
            // in-sync replica set.
            Map<ReplicaState, Set<TopicPartition>> replicas =
                replicaStates.getOrDefault(brokerId, Collections.emptyMap());
            Set<TopicPartition> topicParts =
                replicas.getOrDefault(ReplicaState.IN_SYNC, Collections.emptySet());
            if (!topicParts.isEmpty()) {
                // Remove all replica entries for the removed broker.
                // TODO: include dirty vs. clean election configuration here
                isrDelta.replicaRemovals.add(new ReplicaRemoval(brokerId,
                    new HashSet<>(topicParts)));
            }
        }
    }

    /**
     * Handle changes to topics and partitions.
     *
     * @param delta     The delta to apply.
     */
    public void handleTopicUpdates(TopicDelta delta) {
        delta.apply(state.topics());
    }

//        // Process deleted brokers.
//        for (int brokerId : deletedBrokerIds) {
//            Map<ReplicaState, Set<TopicPartition>> statesToParts =
//                replicaStates.getOrDefault(brokerId, Collections.emptyMap());
//            for (ReplicaState replicaState : Arrays.asList(ReplicaState.ONLINE)) {
//                Set<TopicPartition> parts = statesToParts.get(replicaState);
//                if (parts != null) {
//                    Set<TopicPartition> offlineParts = statesToParts.computeIfAbsent(
//                        ReplicaState.OFFLINE, __ -> new HashSet<>());
//                    for (Iterator<TopicPartition> iter = parts.iterator();
//                             iter.hasNext(); ) {
//                        TopicPartition topicPart = iter.next();
//                        offlineParts.add(topicPart);
//                        iter.remove();
//                        MetadataState.Partition part = findPart(topicPart);
//                        TopicDelta.IsrChange isr = currentIsr(topicPart, part, topicDelta);
//                        TopicDelta.IsrChange newIsr =
//                            calculateNewIsrWithReplicaRemoved(isr, brokerId);
//                        if (!isr.equals(newIsr)) {
//                            topicDelta.isrChanges.put(topicPart, newIsr);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    MetadataState.Partition findPart(TopicPartition topicPart) {
//        MetadataState.Topic topic = state.topics().find(topicPart.topic());
//        if (topic == null) {
//            throw new RuntimeException("No such topic as " + topicPart.topic());
//        }
//        MetadataState.Partition part = topic.partitions().find(topicPart.partition());
//        if (part == null) {
//            throw new RuntimeException("No such partition as " + topicPart);
//        }
//        return part;
//    }
//
//    TopicDelta.IsrChange currentIsr(TopicPartition topicPart,
//                                    MetadataState.Partition part,
//                                    TopicDelta topicDelta) {
//        TopicDelta.IsrChange isrChange = topicDelta.isrChanges.get(topicPart);
//        if (isrChange != null) {
//            return isrChange;
//        }
//        return new TopicDelta.IsrChange(part.leader(), part.leaderEpoch(), part.isr(),
//            part.controllerEpochOfLastIsrUpdate());
//    }
//
//    TopicDelta.IsrChange calculateNewIsrWithReplicaRemoved(MetadataState.Partition part,
//                                                           TopicDelta.IsrChange currentIsr,
//                                                           int replicaToRemove) {
//        List<Integer> newIsr = new ArrayList<>();
//        for (int replica : currentIsr.isr) {
//            if (replica != replicaToRemove) {
//                newIsr.add(replica);
//            }
//        }
//        if (newIsr.equals(currentIsr.isr)) {
//            return currentIsr;
//        }
//        int newLeader;
//        if (replicaToRemove == currentIsr.leader) {
//            newLeader = chooseBestLeader(part, newIsr);
//        } else {
//            newLeader = currentIsr.leader;
//        }
//        return new TopicDelta.IsrChange(
//            newLeader, currentIsr.leaderEpoch + 1, newIsr, controllerEpoch);
//    }
//
//    // how to handle
//
//    int chooseBestLeader(MetadataState.Partition part) {
//
//    }
//
//    private void createIsrRemovalDelta(TopicPartition topicPart,
//                                       int replicaToRemove,
//                                       TopicDelta topicDelta) {
//            topicDelta.replicaChanges.put(topicPart, new TopicDelta.IsrChange()
//
//
//                IsrChange(int leader, int leaderEpoch, List<Integer> isr, int controllerEpoch) {
//
//            }
//                newIsr, Collections.emptyList(), Collections.emptyList()));
//        } else {
//            List<Integer> newReplicaList = new ArrayList<>();
//            for (int replica : replicaChange.replicas) {
//                if (replica != replicaToRemove) {
//                    newReplicaList.add(replica);
//                }
//            }
//            topicDelta.replicaChanges.put(topicPart, new TopicDelta.ReplicaChange(
//                newReplicaList, replicaChange.addingReplicas, replicaChange.removingReplicas));
//        }
//    }
//
//    private TopicDelta.IsrChange calculateLeader(TopicPartition topicPart,
//                                                 int replicaToRemove,
//                                                 TopicDelta topicDelta) {
//
//    }
//
//
//    enum ReplicaState {
//        OFFLINE,
//        ONLINE
//    }
//
//    // map brokers to states to partitions
//    private final Map<Integer, Map<ReplicaState, Set<TopicPartition>>>
//        replicaStates = new HashMap<>();
}
