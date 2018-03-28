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

package org.apache.kafka.soak.tool;

import org.apache.kafka.soak.cloud.MockCloud;
import org.apache.kafka.soak.cloud.MockCommandResponse;
import org.apache.kafka.soak.cluster.MiniSoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.cluster.TestFixture;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.role.BrokerRole;
import org.apache.kafka.soak.role.ZooKeeperRole;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.kafka.soak.role.TrogdorDaemonType.AGENT;
import static org.apache.kafka.soak.role.TrogdorDaemonType.COORDINATOR;
import static org.junit.Assert.assertEquals;

public class SoakStatusTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testSoakStatus() throws Throwable {
        try (MiniSoakCluster miniCluster = TestFixture.createRoleTestCluster()) {
            final MockCloud cloud = miniCluster.cloud();
            // Set up BrokerStatus command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("broker0", "broker1", "broker2")) {
                cloud.addCommandResponse(node.nodeName(),
                    new MockCommandResponse(0, SoakUtil.checkJavaProcessStatusArgs(
                        BrokerRole.KAFKA_CLASS_NAME)));
            }
            // Set up ZooKeeperStatus command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("zk0", "zk1", "zk2")) {
                cloud.addCommandResponse(node.nodeName(),
                    new MockCommandResponse(0, SoakUtil.checkJavaProcessStatusArgs(
                        ZooKeeperRole.ZOOKEEPER_CLASS_NAME)));
            }
            // Set up Trogdor coordinator status command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("trogdor0")) {
                cloud.addCommandResponse(node.nodeName(),
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(COORDINATOR.className())));
            }
            // Set up TrogdorStatus command line responses for agent nodes.
            for (SoakNode node : miniCluster.cluster().
                nodes("broker0", "broker1", "broker2", "zk0", "zk1", "zk2")) {
                cloud.addCommandResponse(node.nodeName(),
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(AGENT.className())));
            }
            assertEquals(0, SoakStatus.
                getStatuses(miniCluster.cluster(), miniCluster.soakEnvironment()));
        }
    }
}
