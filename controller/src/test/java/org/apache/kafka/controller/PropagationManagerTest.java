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

import kafka.server.KafkaConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class PropagationManagerTest {
    private static final Logger log =
        LoggerFactory.getLogger(PropagationManagerTest.class);

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
        try (PropagatorUnitTestEnv env =
                 new PropagatorUnitTestEnv("testInitialEndpointUpdate")) {
            ReplicationManager replicationManager = createTestReplicationManager();
            env.propagationManager().initialize(replicationManager);
            assertEquals(null, env.propagationManager().nodes());
            assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)),
                env.propagationManager().brokers().keySet());
            assertEquals(Arrays.asList(new Node(0, "host0", 9093, "rack0"),
                                       new Node(1, "host1", 9093, "rack1"),
                                       new Node(2, "host2", 9093, "rack0")),
                env.propagationManager().recalculateNodes(replicationManager));
        }
    }
}
