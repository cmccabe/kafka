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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PropagationManagerTest {
    private static final Logger log =
        LoggerFactory.getLogger(PropagationManagerTest.class);

    static class MockPropagationManagerCallbackHandler
        implements PropagationManagerCallbackHandler {
        @Override
        public void handleLeaderAndIsrResponse(int controllerEpoch, int brokerId,
                                               ClientResponse response) {
        }

        @Override
        public void handleUpdateMetadataResponse(int controllerEpoch, int brokerId,
                                                 ClientResponse response) {
        }
    }

    static class TestEnv {
        private final ControllerLogContext logContext;
        private final KafkaConfig config;
        private final MockPropagator propagator;
        private final PropagationManager propagationManager;
        private final MockPropagationManagerCallbackHandler callbackHandler;

        public TestEnv(String name) {
            this.logContext = ControllerLogContext.fromPrefix(name);
            this.config = ControllerTestUtils.newKafkaConfig(0,
                KafkaConfig.ControlPlaneListenerNameProp(), "CONTROLLER",
                KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
                KafkaConfig.ListenersProp(), "CONTROLLER://localhost:9093,INTERNAL://localhost:9092",
                KafkaConfig.ListenerSecurityProtocolMapProp(), "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT");
            this.propagator = new MockPropagator();
            this.callbackHandler = new MockPropagationManagerCallbackHandler();
            this.propagationManager = new PropagationManager(logContext, 0, 0,
                callbackHandler, config);
        }
    }

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCalculateTargetListener() {
        KafkaConfig config = ControllerTestUtils.newKafkaConfig(0,
            KafkaConfig.ControlPlaneListenerNameProp(), "CONTR",
            KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
            KafkaConfig.ListenersProp(), "CONTR://localhost:9093,INTERNAL://localhost:9092",
            KafkaConfig.ListenerSecurityProtocolMapProp(), "CONTR:PLAINTEXT,INTERNAL:PLAINTEXT");
        assertEquals("CONTR", PropagationManager.calculateTargetListener(config));
    }

    @Test
    public void testCalculateTargetListenerWithInterBrokerListenerName() {
        KafkaConfig config = ControllerTestUtils.newKafkaConfig(0,
            KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
            KafkaConfig.ListenersProp(), "INTERNAL://localhost:9092",
            KafkaConfig.ListenerSecurityProtocolMapProp(), "INTERNAL:PLAINTEXT");
        assertEquals("INTERNAL", PropagationManager.calculateTargetListener(config));
    }

    private static MetadataState.BrokerCollection newBrokers() {
        MetadataState.BrokerCollection brokers = new MetadataState.BrokerCollection();
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            MetadataState.Broker broker = brokers.getOrCreate(brokerId);
            broker.setBrokerEpoch(123 + brokerId);
            broker.endPoints().getOrCreate("PLAINTEXT").
                setHost("host" + brokerId).setPort((short) 9092).
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);
            broker.endPoints().getOrCreate("CONTROLLER").
                setHost("host" + brokerId).setPort((short) 9093).
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);
            if ((brokerId % 2) == 0) {
                broker.setRack("rack0");
            } else {
                broker.setRack("rack1");
            }
        }
        return brokers;
    }

    private static ReplicationManager createTestReplicationManager() {
        MetadataState state = new MetadataState().setBrokers(newBrokers());
        state.topics().getOrCreate("foo").partitions().
            getOrCreate(0).setLeader(0).setControllerEpochOfLastIsrUpdate(0).
            setLeaderEpoch(100).setReplicas(Arrays.asList(0, 1, 2)).
            setIsr(Arrays.asList(0, 1, 2)).setZkVersionOfLastIsrUpdate(0);
        state.topics().getOrCreate("foo").partitions().
            getOrCreate(1).setLeader(2).setControllerEpochOfLastIsrUpdate(0).
            setLeaderEpoch(100).setReplicas(Arrays.asList(2, 0, 1)).
            setIsr(Arrays.asList(2, 0, 1)).setZkVersionOfLastIsrUpdate(0);
        state.topics().getOrCreate("bar").partitions().
            getOrCreate(0).setLeader(1).setControllerEpochOfLastIsrUpdate(0).
            setLeaderEpoch(100).setReplicas(Arrays.asList(1, 0, 2)).
            setIsr(Arrays.asList(1, 0)).setZkVersionOfLastIsrUpdate(0);
        ReplicationManager replicationManager = new ReplicationManager(state);
        return replicationManager;
    }

    @Test
    public void testBrokerToNode() {
        MetadataState.Broker broker = newBrokers().find(1);
        assertEquals(new Node(1, "host1", 9093, "rack1"),
            PropagationManager.brokerToNode(broker, "CONTROLLER"));
    }

    @Test
    public void testBrokerToNodeFailure() {
        MetadataState.Broker broker = newBrokers().find(0);
        assertEquals(null, PropagationManager.brokerToNode(broker, "BLAH"));
    }

    @Test
    public void testInitializePropagationManager() throws Exception {
        TestEnv env = new TestEnv("testInitialEndpointUpdate");
        ReplicationManager replicationManager = createTestReplicationManager();
        env.propagationManager.initialize(replicationManager);
        assertEquals(null, env.propagationManager.nodes());
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
            env.propagationManager.brokers().keySet());
        assertEquals(Arrays.asList(new Node(0, "host0", 9093, "rack0"),
            new Node(1, "host1", 9093, "rack1"),
            new Node(2, "host2", 9093, "rack0")),
            env.propagationManager.recalculateNodes(replicationManager));
    }

    static Map<TopicPartition, UpdateMetadataPartitionState>
            umrMap(Iterable<UpdateMetadataPartitionState> i) {
        Iterator<UpdateMetadataPartitionState> iter = i.iterator();
        Map<TopicPartition, UpdateMetadataPartitionState> map = new HashMap<>();
        while (iter.hasNext()) {
            UpdateMetadataPartitionState part = iter.next();
            map.put(new TopicPartition(part.topicName(), part.partitionIndex()), part);
        }
        return map;
    }

    static Map<TopicPartition, LeaderAndIsrPartitionState>
            lairMap(Iterable<LeaderAndIsrPartitionState> i) {
        Iterator<LeaderAndIsrPartitionState> iter = i.iterator();
        Map<TopicPartition, LeaderAndIsrPartitionState> map = new HashMap<>();
        while (iter.hasNext()) {
            LeaderAndIsrPartitionState part = iter.next();
            map.put(new TopicPartition(part.topicName(), part.partitionIndex()), part);
        }
        return map;
    }

    @Test
    public void testGenerateInitialMessages() throws Exception {
        TestEnv env = new TestEnv("testGenerateInitialMessages");
        ReplicationManager replicationManager = createTestReplicationManager();
        env.propagationManager.initialize(replicationManager);
        assertEquals(0, env.propagator.numInFlight());
        env.propagationManager.maybeSendRequests(100L, replicationManager, env.propagator);
        for (Map.Entry<Integer, PropagationManager.DestinationBroker> entry :
                env.propagationManager.brokers().entrySet()) {
            PropagationManager.DestinationBroker broker = entry.getValue();
            assertEquals(entry.getKey(), Integer.valueOf(broker.id));
            assertEquals(null, broker.pendingUpdateMetadata);
            assertNotNull(broker.inFlightUpdateMetadata);
            assertEquals(100L, broker.inFlightUpdateMetadata.sendTimeNs());
            assertEquals(null, broker.pendingLeaderAndIsr);
            assertEquals(100L, broker.inFlightLeaderAndIsr.sendTimeNs());
            assertNotNull(broker.inFlightLeaderAndIsr);
            RequestAndCompletionHandler umrRequest =
                env.propagator.getInFlightRequest(broker.id, ApiKeys.UPDATE_METADATA);
            assertNotNull(umrRequest);
            UpdateMetadataRequest umrReq = (UpdateMetadataRequest) umrRequest.request().build();
            assertEquals(umrMap(Arrays.asList(
                new UpdateMetadataPartitionState().setTopicName("foo").
                    setPartitionIndex(0).setLeader(0).setControllerEpoch(0).
                    setLeaderEpoch(100).setReplicas(Arrays.asList(0, 1, 2)).
                    setIsr(Arrays.asList(0, 1, 2)),
                new UpdateMetadataPartitionState().setTopicName("foo").
                        setPartitionIndex(1).setLeader(2).setControllerEpoch(0).
                        setLeaderEpoch(100).setReplicas(Arrays.asList(2, 0, 1)).
                        setIsr(Arrays.asList(2, 0, 1)),
                new UpdateMetadataPartitionState().setTopicName("bar").
                    setPartitionIndex(0).setLeader(1).setControllerEpoch(0).
                    setLeaderEpoch(100).setReplicas(Arrays.asList(1, 0, 2)).
                    setIsr(Arrays.asList(1, 0)))),
                umrMap(umrReq.partitionStates()));

            RequestAndCompletionHandler lairRequest =
                env.propagator.getInFlightRequest(broker.id, ApiKeys.LEADER_AND_ISR);
            assertNotNull(lairRequest);
            LeaderAndIsrRequest lairReq = (LeaderAndIsrRequest) lairRequest.request().build();
            assertEquals(lairMap(Arrays.asList(
                new LeaderAndIsrPartitionState().setTopicName("foo").
                    setPartitionIndex(0).setLeader(0).setControllerEpoch(0).
                    setLeaderEpoch(100).setReplicas(Arrays.asList(0, 1, 2)).
                    setIsr(Arrays.asList(0, 1, 2)),
                new LeaderAndIsrPartitionState().setTopicName("foo").
                    setPartitionIndex(1).setLeader(2).setControllerEpoch(0).
                    setLeaderEpoch(100).setReplicas(Arrays.asList(2, 0, 1)).
                    setIsr(Arrays.asList(2, 0, 1)),
                new LeaderAndIsrPartitionState().setTopicName("bar").
                    setPartitionIndex(0).setLeader(1).setControllerEpoch(0).
                    setLeaderEpoch(100).setReplicas(Arrays.asList(1, 0, 2)).
                    setIsr(Arrays.asList(1, 0)))),
                lairMap(lairReq.partitionStates()));
        }
    }
}
