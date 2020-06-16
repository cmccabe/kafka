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

public final class ReplicationManager {
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

//    private final ControllerLogContext logContext;
//    private final Logger log;
//    private final int controllerEpoch;
//    private final BackingStore backingStore;
//    private final EventQueue mainQueue;
//    private final MetadataState state;
//
//    KafkaController(ControllerLogContext logContext,
//                    int controllerEpoch,
//                    BackingStore backingStore,
//                    EventQueue mainQueue,
//                    MetadataState state) {
//        this.logContext = logContext;
//        this.log = new LogContext(logContext.logContext().logPrefix() +
//            " [epoch " + controllerEpoch + "] ").logger(KafkaController.class);
//        this.controllerEpoch = controllerEpoch;
//        this.backingStore = backingStore;
//        this.mainQueue = mainQueue;
//        this.state = state;
//    }
//
//    public int controllerEpoch() {
//        return controllerEpoch;
//    }
//
//    static class PropagatorDelta {
//        private final Map<Integer, PropagatorBrokerDelta> brokerDeltas = new HashMap<>();
//
//        private void addFullBrokerUpdate(int brokerId) {
//            brokerDeltas.put(brokerId, new PropagatorBrokerDelta(true));
//        }
//    }
//
//    static class PropagatorBrokerDelta {
//        private final boolean full;
//
//        PropagatorBrokerDelta(boolean full) {
//            this.full = full;
//        }
//    }
//
//    public void handleBrokerUpdates(List<MetadataState.Broker> changedBrokers,
//                                    List<Integer> deletedBrokerIds,
//                                    PropagatorDelta propagatorDelta,
//                                    TopicDelta topicDelta) {
//        // Process changed brokers.
//        for (MetadataState.Broker changedBroker : changedBrokers) {
//            MetadataState.Broker existingBroker =
//                state.brokers().find(changedBroker.brokerId());
//            // TODO: what does this mean for the topicDelta?  Do we have to do anything here?
//            if (existingBroker == null) {
//                state.brokers().mustAdd(changedBroker);
//                // Send out a full update to the new broker.
//                propagatorDelta.addFullBrokerUpdate(changedBroker.brokerId());
//            } else {
//                if (existingBroker.brokerEpoch() != changedBroker.brokerEpoch()) {
//                    // The broker was bounced, so we should send out a full update.
//                    propagatorDelta.addFullBrokerUpdate(existingBroker.brokerId());
//                }
//                // Update our cached information.
//                state.brokers().remove(existingBroker);
//            }
//            state.brokers().mustAdd(changedBroker);
//        }
//
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
