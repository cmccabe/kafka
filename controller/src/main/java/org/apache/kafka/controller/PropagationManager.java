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

import kafka.common.RequestAndCompletionHandler;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.MetadataState.Broker;
import org.apache.kafka.common.message.MetadataState.BrokerEndpoint;
import org.apache.kafka.common.message.MetadataState.Partition;
import org.apache.kafka.common.message.MetadataState.Topic;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.controller.TopicDelta.ReplicaChange;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * The PropagationManager manages sending out LeaderAndIsr and UpdateMetadata messages to
 * the rest of the cluster.
 *
 * Its methods are intended to be called from the KafkaControllerManager's main queue
 * thread.  Therefore, there is no internal synchronization.
 */
public class PropagationManager {
    /**
     * Determine what listener to target.
     *
     * @param config    The Kafka server configuration.
     *
     * @return          The name of the listener to send our messages to.
     */
    static String calculateTargetListener(KafkaConfig config) {
        ListenerName name = OptionConverters.toJava(config.controlPlaneListenerName()).
            orElseGet(() -> config.interBrokerListenerName());
        return name.value();
    }

    /**
     * Convert a Broker object to a Node object suitable for use with
     * a NetworkClient.
     *
     * @param broker            The input broker object.
     * @param targetListener    The listener we want to communicate with.
     *
     * @return                  The node object, or null if the target listener was
     *                          not found.
     */
    static Node brokerToNode(Broker broker, String targetListener) {
        BrokerEndpoint endpoint = broker.endPoints().find(targetListener);
        if (endpoint == null) {
            return null;
        }
        return new Node(broker.brokerId(), endpoint.host(), endpoint.port(),
                broker.rack());
    }

    public class SimpleUpdateMetadataRequestBuilder
            extends AbstractRequest.Builder<UpdateMetadataRequest> {
        private final UpdateMetadataRequestData data;

        public SimpleUpdateMetadataRequestBuilder(UpdateMetadataRequestData data) {
            super(ApiKeys.UPDATE_METADATA, updateMetadataRequestVersion);
            this.data = data;
        }

        @Override
        public UpdateMetadataRequest build(short version) {
            return new UpdateMetadataRequest(data, version);
        }
    }

    class UpdateMetadataCompletionHandler implements RequestCompletionHandler {
        private final int brokerId;

        UpdateMetadataCompletionHandler(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public void onComplete(ClientResponse response) {
            callbackHandler.handleUpdateMetadataResponse(controllerEpoch,
                brokerId, response);
        }
    }

    public class SimpleLeaderAndIsrRequestBuilder
            extends AbstractRequest.Builder<LeaderAndIsrRequest> {
        private final LeaderAndIsrRequestData data;

        public SimpleLeaderAndIsrRequestBuilder(LeaderAndIsrRequestData data) {
            super(ApiKeys.LEADER_AND_ISR, leaderAndIsrRequestVersion);
            this.data = data;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            return new LeaderAndIsrRequest(data, version);
        }
    }

    class LeaderAndIsrCompletionHandler implements RequestCompletionHandler {
        private final int brokerId;

        LeaderAndIsrCompletionHandler(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public void onComplete(ClientResponse response) {
            callbackHandler.handleLeaderAndIsrResponse(controllerEpoch,
                brokerId, response);
        }
    }

    /**
     * The ControllerLogContext which this object shares with the rest of the controller.
     */
    private final ControllerLogContext logContext;

    /**
     * The epoch of the current controller.
     */
    private final int controllerEpoch;

    /**
     * The id of the current controller.
     */
    private final int controllerId;

    /**
     * Handles callbacks from this manager.
     */
    private final PropagationManagerCallbackHandler callbackHandler;

    /**
     * The logger to use.
     */
    private final Logger log;

    /**
     * The listener name to use to when contacting other brokers.
     * Since brokers can have multiple listeners, corresponding to multiple open ports,
     * we need to know which ones to connect to.
     */
    private final String targetListener;

    /**
     * The amount of time to delay in nanoseconds before sending partial updates.
     * The delay helps reduce the number of messages sent by coalescing multiple updates
     * into one.
     */
    private final long coalesceDelayNs;

    /**
     * The version of LeaderAndIsrRequest to send out.
     */
    private final short leaderAndIsrRequestVersion;

    /**
     * The version of UpdateMetadataRequest to send out.
     */
    private final short updateMetadataRequestVersion;

    /**
     * Propagation information for all brokers.
     */
    private final Map<Integer, DestinationBroker> brokers;

    /**
     * The node metadata, or null if the node metadata needs to be recalculated.
     */
    private List<Node> nodes;

    /**
     * A request that we would like to send, but haven't sent yet.
     */
    static class PendingRequest {
        private final Set<String> topics;

        private final long earliestSendTimeNs;

        /**
         * Create a pending full request.
         */
        PendingRequest() {
            this.topics = null;
            this.earliestSendTimeNs = 0;
        }

        /**
         * Create a pending partial request.
         */
        PendingRequest(long earliestSendTimeNs) {
            this.topics = new HashSet<>();
            this.earliestSendTimeNs = earliestSendTimeNs;
        }

        /**
         * Create a pending partial or full request.
         */
        PendingRequest(Set<String> topics, long earliestSendTimeNs) {
            this.topics = topics;
            this.earliestSendTimeNs = earliestSendTimeNs;
        }

        boolean isFull() {
            return topics == null;
        }

        public Set<String> topics() {
            return topics;
        }

        public long earliestSendTimeNs() {
            return earliestSendTimeNs;
        }

        public void addTopicDelta(TopicDelta delta) {
            if (isFull()) {
                // There is no need to track specific topics if we're doing a full update.
                return;
            }
            for (Topic topic : delta.addedTopics) {
                topics.add(topic.name());
            }
            // note: removedTopics actually does not get added to the update.
            // TODO: add removING topics to the update.
            for (TopicPartition topicPart : delta.addedParts.keySet()) {
                topics.add(topicPart.topic());
            }
            for (TopicPartition topicPart : delta.removedParts) {
                topics.add(topicPart.topic());
            }
            for (TopicPartition topicPart : delta.replicaChanges.keySet()) {
                topics.add(topicPart.topic());
            }
            for (TopicPartition topicPart : delta.isrChanges.keySet()) {
                topics.add(topicPart.topic());
            }
        }
    }

    /**
     * A request which we are in the process of sending.
     */
    static class InFlightRequest {
        private final Set<String> topics;

        private final long sendTimeNs;

        InFlightRequest(Set<String> topics, long sendTimeNs) {
            this.topics = topics;
            this.sendTimeNs = sendTimeNs;
        }

        long sendTimeNs() {
            return sendTimeNs;
        }
    }

    /**
     * Propagation information about a specific broker.
     */
    static class DestinationBroker {
        /**
         * The broker id.
         */
        final int id;

        /**
         * The pending update metadata request, or null if there is none.
         */
        PendingRequest pendingUpdateMetadata = new PendingRequest();

        /**
         * The in-flight UpdateMetadataRequest, or null if there is none.
         */
        InFlightRequest inFlightUpdateMetadata = null;

        /**
         * The pending leader and isr request, or null if there is none.
         */
        PendingRequest pendingLeaderAndIsr = new PendingRequest();

        /**
         * The in-flight LeaderAndIsrRequest, or null if there is none.
         */
        InFlightRequest inFlightLeaderAndIsr = null;

        DestinationBroker(int id) {
            this.id = id;
        }
    }

    PropagationManager(ControllerLogContext logContext,
                       int controllerEpoch,
                       int controllerId,
                       PropagationManagerCallbackHandler callbackHandler,
                       KafkaConfig config) {
        this.logContext = logContext;
        this.controllerEpoch = controllerEpoch;
        this.controllerId = controllerId;
        this.callbackHandler = callbackHandler;
        this.log = logContext.createLogger(PropagationManager.class);
        this.targetListener = calculateTargetListener(config);
        this.coalesceDelayNs = config.controllerPropagationDelayNs();
        this.updateMetadataRequestVersion =
            config.interBrokerProtocolVersion().updateMetadataRequestVersion();
        this.leaderAndIsrRequestVersion =
            config.interBrokerProtocolVersion().leaderAndIsrRequestVersion();
        this.brokers = new HashMap<>();
        this.nodes = null;
    }

    public void initialize(ReplicationManager replicationManager) {
        for (Broker broker : replicationManager.state().brokers()) {
            brokers.put(broker.brokerId(), new DestinationBroker(broker.brokerId()));
        }
    }

    public void maybeSendRequests(long nowNs,
                                  ReplicationManager replicationManager,
                                  Propagator propagator) {
        if (nodes == null) {
            nodes = recalculateNodes(replicationManager);
        }
        ArrayList<RequestAndCompletionHandler> newRequests = new ArrayList<>();
        for (DestinationBroker broker : brokers.values()) {
            maybeCreateUpdateMetadata(nowNs, newRequests, replicationManager, broker);
            maybeCreateLeaderAndIsr(nowNs, newRequests, replicationManager, broker);
        }
        propagator.send(nodes, newRequests);
    }

    /**
     * Get the next time in absolute nanoseconds when we should invoke maybeSendRequests.
     */
    public long nextSendTimeNs() {
        long nextSendTimeNs = Long.MAX_VALUE;
        for (DestinationBroker broker : brokers.values()) {
            if (broker.pendingUpdateMetadata != null) {
                nextSendTimeNs = Math.min(nextSendTimeNs,
                    broker.pendingUpdateMetadata.earliestSendTimeNs);
            }
            if (broker.pendingLeaderAndIsr != null) {
                nextSendTimeNs = Math.min(nextSendTimeNs,
                    broker.pendingLeaderAndIsr.earliestSendTimeNs);
            }
        }
        return nextSendTimeNs;
    }

    /**
     * Recalculate node information based on the set of all current brokers.
     */
    List<Node> recalculateNodes(ReplicationManager replicationManager) {
        List<Node> nodes = new ArrayList<>();
        for (Broker broker : replicationManager.state().brokers()) {
            Node node = brokerToNode(broker, targetListener);
            if (node == null) {
                throw new RuntimeException("Broker " + broker.brokerId() + " doesn't " +
                    "have a " + targetListener + " listener, but we are configured " +
                    "to send controller messages to that listener.");
            }
            nodes.add(node);
        }
        return nodes;
    }

    private void maybeCreateUpdateMetadata(long nowNs,
                                           List<RequestAndCompletionHandler> requests,
                                           ReplicationManager replicationManager,
                                           DestinationBroker destBroker) {
        if (destBroker.inFlightUpdateMetadata != null) {
            log.trace("Broker {} already has an in-flight update metadata request.",
                destBroker.id);
            return;
        }
        if (destBroker.pendingUpdateMetadata == null) {
            log.trace("Broker {} does not need to send a new update metadata request.",
                destBroker.id);
            return;
        }
        if (nowNs < destBroker.pendingUpdateMetadata.earliestSendTimeNs) {
            log.trace("Broker {} is still waiting to send an update metadata request.",
                destBroker.id);
            return;
        }
        MetadataState state = replicationManager.state();
        Broker destBrokerState = state.brokers().find(destBroker.id);
        if (destBrokerState == null) {
            throw new RuntimeException("Internal logic error: DestinationBroker map " +
                "contains broker " + destBroker.id + ", but the ReplicationManager state " +
                "does not.");
        }
        UpdateMetadataRequestData data = new UpdateMetadataRequestData();
        data.setControllerId(controllerId);
        data.setControllerEpoch(controllerEpoch);
        data.setBrokerEpoch(destBrokerState.brokerEpoch());
        for (Broker brokerState : state.brokers()) {
            UpdateMetadataRequestData.UpdateMetadataBroker liveBroker =
                new UpdateMetadataRequestData.UpdateMetadataBroker();
            liveBroker.setId(brokerState.brokerId());
            if (updateMetadataRequestVersion == 0) {
                // v0 is very old.  It only supports a single plaintext endpoint per broker.
                BrokerEndpoint endpoint = brokerState.endPoints().find(
                    SecurityProtocol.PLAINTEXT.name);
                if (endpoint == null) {
                    throw new RuntimeException("UpdateMetadataRequest version is 0, " +
                        "but there is no PLAINTEXT endpoint for broker " +
                        brokerState.brokerId());
                }
                liveBroker.setV0Host(endpoint.host());
                liveBroker.setV0Port(endpoint.port());
            } else {
                for (BrokerEndpoint endpoint : brokerState.endPoints()) {
                    liveBroker.endpoints().add(
                        new UpdateMetadataRequestData.UpdateMetadataEndpoint().
                            setPort(endpoint.port()).
                            setHost(endpoint.host()).
                            setListener(endpoint.name()).
                            setSecurityProtocol(endpoint.securityProtocol()));
                }
            }
            liveBroker.setRack(brokerState.rack());
            data.liveBrokers().add(liveBroker);
        }
        if (destBroker.pendingUpdateMetadata.isFull()) {
            for (Topic topic : state.topics()) {
                createUmrTopicEntry(data, topic);
            }
            log.debug("Creating new full UpdateMetadataRequest for {} with {} topic(s).",
                destBroker.id, state.topics().size());
        } else {
            for (String topicName : destBroker.pendingUpdateMetadata.topics) {
                Topic topic = state.topics().find(topicName);
                if (topic == null) {
                    throw new RuntimeException("Unable to locate topic " + topicName +
                        " in the ReplicationManager.");
                }
                createUmrTopicEntry(data, topic);
            }
            log.debug("Creating new incremental UpdateMetadataRequest for {} with {} " +
                "topic(s).", destBroker.id, destBroker.pendingUpdateMetadata.topics.size());
        }
        destBroker.inFlightUpdateMetadata =
            new InFlightRequest(destBroker.pendingUpdateMetadata.topics(), nowNs);
        destBroker.pendingUpdateMetadata = null;
        requests.add(new RequestAndCompletionHandler(
            brokerToNode(destBrokerState, targetListener),
            new SimpleUpdateMetadataRequestBuilder(data),
            new UpdateMetadataCompletionHandler(destBroker.id)));
    }

    private void createUmrTopicEntry(UpdateMetadataRequestData data, Topic topic) {
        if (updateMetadataRequestVersion >= 5) {
            UpdateMetadataRequestData.UpdateMetadataTopicState topicState =
                new UpdateMetadataRequestData.UpdateMetadataTopicState();
            topicState.setTopicName(topic.name());
            for (Partition partition : topic.partitions()) {
                topicState.partitionStates().add(
                    createUmrPartitionEntry(topic, partition));
            }
            data.topicStates().add(topicState);
        } else {
            for (Partition partition : topic.partitions()) {
                data.ungroupedPartitionStates().add(
                    createUmrPartitionEntry(topic, partition));
            }
        }
    }

    private static UpdateMetadataRequestData.UpdateMetadataPartitionState
            createUmrPartitionEntry(Topic topic, Partition partition) {
        UpdateMetadataRequestData.UpdateMetadataPartitionState part =
            new UpdateMetadataRequestData.UpdateMetadataPartitionState();
        part.setTopicName(topic.name()); // only used when version <= 4
        part.setPartitionIndex(partition.id());
        part.setControllerEpoch(partition.controllerEpochOfLastIsrUpdate());
        part.setLeader(partition.leader());
        part.setLeaderEpoch(partition.leaderEpoch());
        // TODO: we need to set the ISR and some other fields to something special if RM
        //  says we're deleting this topic.
        part.setIsr(new ArrayList<>(partition.isr()));
        part.setZkVersion(partition.zkVersionOfLastIsrUpdate());
        part.setReplicas(new ArrayList<>(partition.replicas()));
        // part.setOfflineReplicas(...); //TODO: need to get this from ReplicationManager
        return part;
    }

    private void maybeCreateLeaderAndIsr(long nowNs,
                                         List<RequestAndCompletionHandler> requests,
                                         ReplicationManager replicationManager,
                                         DestinationBroker destBroker) {
        if (destBroker.inFlightLeaderAndIsr != null) {
            log.trace("Broker {} already has an in-flight leader and isr request.",
                destBroker.id);
            return;
        }
        if (destBroker.pendingLeaderAndIsr == null) {
            log.trace("Broker {} does not need to send a new leader and isr request.",
                destBroker.id);
            return;
        }
        if (nowNs < destBroker.pendingLeaderAndIsr.earliestSendTimeNs) {
            log.trace("Broker {} is still waiting to send a leader and isr request.",
                destBroker.id);
            return;
        }
        MetadataState state = replicationManager.state();
        Broker destBrokerState = state.brokers().find(destBroker.id);
        if (destBrokerState == null) {
            throw new RuntimeException("Internal logic error: DestinationBroker map " +
                "contains broker " + destBroker.id + ", but the ReplicationManager state " +
                "does not.");
        }
        LeaderAndIsrRequestData data = new LeaderAndIsrRequestData();
        data.setControllerId(controllerId);
        data.setControllerEpoch(controllerEpoch);
        data.setBrokerEpoch(destBrokerState.brokerEpoch());
        for (Broker brokerState : state.brokers()) {
            LeaderAndIsrRequestData.LeaderAndIsrLiveLeader liveLeader =
                new LeaderAndIsrRequestData.LeaderAndIsrLiveLeader();
            liveLeader.setBrokerId(brokerState.brokerId());
            BrokerEndpoint endpoint = brokerState.endPoints().find(targetListener);
            if (endpoint == null) {
                throw new RuntimeException("There is no " + targetListener + " endpoint " +
                    "for broker " + brokerState.brokerId());
            }
            liveLeader.setBrokerId(brokerState.brokerId());
            liveLeader.setHostName(endpoint.host());
            liveLeader.setPort(endpoint.port());
            data.liveLeaders().add(liveLeader);
        }
        if (destBroker.pendingLeaderAndIsr.isFull()) {
            for (Topic topic : state.topics()) {
                createIsrTopicEntry(data, topic);
            }
            log.debug("Creating new full LeaderAndIsrRequest for {} with {} topic(s).",
                destBroker.id, state.topics().size());
        } else {
            for (String topicName : destBroker.pendingLeaderAndIsr.topics) {
                Topic topic = state.topics().find(topicName);
                if (topic == null) {
                    throw new RuntimeException("Unable to locate topic " + topicName +
                        " in the ReplicationManager.");
                }
                createIsrTopicEntry(data, topic);
            }
            log.debug("Creating new incremental LeaderAndIsrRequest for {} with {} " +
                "topic(s).", destBroker.id, destBroker.pendingLeaderAndIsr.topics.size());
        }
        destBroker.inFlightLeaderAndIsr =
            new InFlightRequest(destBroker.pendingLeaderAndIsr.topics(), nowNs);
        destBroker.pendingLeaderAndIsr = null;
        requests.add(new RequestAndCompletionHandler(
            brokerToNode(destBrokerState, targetListener),
            new SimpleLeaderAndIsrRequestBuilder(data),
            new LeaderAndIsrCompletionHandler(destBroker.id)));
    }

    private void createIsrTopicEntry(LeaderAndIsrRequestData data, Topic topic) {
        if (leaderAndIsrRequestVersion >= 2) {
            LeaderAndIsrRequestData.LeaderAndIsrTopicState topicState =
                new LeaderAndIsrRequestData.LeaderAndIsrTopicState();
            topicState.setTopicName(topic.name());
            for (Partition partition : topic.partitions()) {
                topicState.partitionStates().add(
                    createIsrPartitionEntry(topic, partition));
            }
            data.topicStates().add(topicState);
        } else {
            for (Partition partition : topic.partitions()) {
                data.ungroupedPartitionStates().add(
                    createIsrPartitionEntry(topic, partition));
            }
        }
    }

    private static LeaderAndIsrRequestData.LeaderAndIsrPartitionState
            createIsrPartitionEntry(Topic topic, Partition partition) {
        LeaderAndIsrRequestData.LeaderAndIsrPartitionState part =
            new LeaderAndIsrRequestData.LeaderAndIsrPartitionState();
        part.setTopicName(topic.name()); // only used when version <= 4
        part.setPartitionIndex(partition.id());
        part.setControllerEpoch(partition.controllerEpochOfLastIsrUpdate());
        part.setLeader(partition.leader());
        part.setLeaderEpoch(partition.leaderEpoch());
        // TODO: we need to set the ISR and some other fields to something special if RM
        //  says we're deleting this topic.
        part.setIsr(new ArrayList<>(partition.isr()));
        part.setZkVersion(partition.zkVersionOfLastIsrUpdate());
        part.setReplicas(new ArrayList<>(partition.replicas()));
        part.setAddingReplicas(new ArrayList<>(partition.addingReplicas()));
        part.setRemovingReplicas(new ArrayList<>(partition.removingReplicas()));
        //part.setIsNew(...); //TODO: need to get this from ReplicationManager
        return part;
    }

    public void handleBrokerUpdates(long nowNs, ReplicationManager replicationManager,
                                    BrokerDelta delta) {
        boolean changed = false;
        for (int brokerId : delta.deletedBrokerIds()) {
            if (!replicationManager.hasInSyncReplicas(brokerId)) {
                // Don't remove the broker until we have removed it from the ISR
                // of all partitions.
                brokers.remove(brokerId);
                changed = true;
            }
        }
        for (Broker broker : delta.changedBrokers()) {
            DestinationBroker destBroker = brokers.get(broker.brokerId());
            if (destBroker == null) {
                destBroker = new DestinationBroker(broker.brokerId());
                brokers.put(broker.brokerId(), destBroker);
                changed = true;
            }
        }
        if (changed) {
            nodes = null; // Invalidate the nodes cache so that we will recalculate it later.
            for (DestinationBroker broker : brokers.values()) {
                if (broker.pendingLeaderAndIsr == null) {
                    broker.pendingLeaderAndIsr = new PendingRequest(nowNs + coalesceDelayNs);
                }
                if (broker.pendingUpdateMetadata == null) {
                    broker.pendingUpdateMetadata = new PendingRequest(nowNs + coalesceDelayNs);
                }
            }
        }
    }

    public void handleTopicUpdates(long nowNs, ReplicationManager replicationManager,
                                   TopicDelta delta) {
        for (DestinationBroker broker : brokers.values()) {
            if (needLeaderAndIsrEntry(broker.id, replicationManager, delta)) {
                if (broker.pendingLeaderAndIsr == null) {
                    broker.pendingLeaderAndIsr = new PendingRequest(nowNs + coalesceDelayNs);
                }
                broker.pendingLeaderAndIsr.addTopicDelta(delta);
            }
            if (broker.pendingUpdateMetadata == null) {
                broker.pendingUpdateMetadata = new PendingRequest(nowNs + coalesceDelayNs);
            }
            broker.pendingUpdateMetadata.addTopicDelta(delta);
        }
    }

    private boolean needLeaderAndIsrEntry(int brokerId, ReplicationManager replicationManager,
                                          TopicDelta delta) {
        for (Topic topic : delta.addedTopics) {
            for (Partition part : topic.partitions()) {
                if (needLeaderAndIsrEntry(brokerId, part)) return true;
            }
        }
        // NOTE: we don't consider removedTopics here.
        // Once a topic has been truly removed it is no longer present on any broker.
        // TODO: handle removING topics by sending the special invalid ISR.
        for (Partition part : delta.addedParts.values()) {
            if (needLeaderAndIsrEntry(brokerId, part)) return true;
        }
        // NOTE: we don't consider or handle removedParts here.  This is OK since Kafka
        // doesn't support this yet (there is an ApiKeys.CREATE_PARTITIONS but no
        // ApiKeys.DELETE_PARTITIONS)
        for (Entry<TopicPartition, ReplicaChange> entry : delta.replicaChanges.entrySet()) {
            if (needLeaderAndIsrEntry(brokerId, replicationManager, entry.getKey())) {
                return true;
            }
            ReplicaChange change = entry.getValue();
            if (change.replicas.contains(brokerId)) return true;
            if (change.removingReplicas.contains(brokerId)) return true;
            if (change.addingReplicas.contains(brokerId)) return true;
        }
        for (TopicPartition topicPart : delta.isrChanges.keySet()) {
            if (needLeaderAndIsrEntry(brokerId, replicationManager, topicPart)) return true;
        }
        return false;
    }

    private boolean needLeaderAndIsrEntry(int brokerId,
                                          ReplicationManager replicationManager,
                                          TopicPartition topicPart) {
        Topic topic = replicationManager.state().topics().find(topicPart.topic());
        Partition part = topic.partitions().find(topicPart.partition());
        return needLeaderAndIsrEntry(brokerId, part);
    }

    private boolean needLeaderAndIsrEntry(int brokerId, Partition part) {
        if (part.replicas().contains(brokerId)) return true;
        if (part.removingReplicas().contains(brokerId)) return true;
        if (part.addingReplicas().contains(brokerId)) return true;
        return false;
    }

    public void completeUpdateMetadataRequest(int brokerId, boolean success) {
        DestinationBroker broker = brokers.get(brokerId);
        if (broker == null) {
            log.debug("Ignoring result of UpdateMetadataRequest to {} since we are " +
                "no longer tracking that broker.", brokerId);
            return;
        }
        if (!success) {
            InFlightRequest inFlightRequest = broker.inFlightUpdateMetadata;
            // TODO: add exponential backoff behavior
            broker.pendingUpdateMetadata =
                new PendingRequest(inFlightRequest.topics, inFlightRequest.sendTimeNs);
        }
        broker.inFlightUpdateMetadata = null;
    }

    public void completeLeaderAndIsr(int brokerId, boolean success) {
        DestinationBroker broker = brokers.get(brokerId);
        if (broker == null) {
            log.debug("Ignoring result of LeaderAndIsrRequest to {} since we are " +
                "no longer tracking that broker.", brokerId);
            return;
        }
        if (!success) {
            InFlightRequest inFlightRequest = broker.inFlightLeaderAndIsr;
            // TODO: add exponential backoff behavior
            broker.pendingLeaderAndIsr =
                new PendingRequest(inFlightRequest.topics, inFlightRequest.sendTimeNs);
        }
        broker.inFlightLeaderAndIsr = null;
    }

    // Visible for testing
    Map<Integer, DestinationBroker> brokers() {
        return brokers;
    }

    // Visible for testing
    List<Node> nodes() {
        return nodes;
    }

    // Visible for testing
    long coalesceDelayNs() {
        return coalesceDelayNs;
    }
}
