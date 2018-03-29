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
import org.apache.kafka.soak.role.BrokerStart;
import org.apache.kafka.soak.role.RolePaths;
import org.apache.kafka.soak.role.RsyncSrcAction;
import org.apache.kafka.soak.role.TrogdorStart;
import org.apache.kafka.soak.role.ZooKeeperRole;
import org.apache.kafka.soak.role.ZooKeeperStart;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.kafka.soak.role.TrogdorDaemonType.AGENT;
import static org.apache.kafka.soak.role.TrogdorDaemonType.COORDINATOR;

public class SoakSetupTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testSoakSetup() throws Throwable {
        try (MiniSoakCluster miniCluster = TestFixture.createRoleTestCluster()) {
            final MockCloud cloud = miniCluster.cloud();
            final String logPath = miniCluster.cluster().logPath();
            // Set up KafkaStart command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("broker0", "broker1", "broker2")) {
                String nodeName = node.nodeName();
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        BrokerRole.KAFKA_CLASS_NAME, true).toArray(new String[0])));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, BrokerStart.createSetupPathsCommandLine()));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, BrokerStart.createRunDaemonCommandLine()));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/broker-%d.properties",
                            logPath, node.nodeIndex()), RolePaths.KAFKA_BROKER_PROPERTIES)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/broker-log4j-%d.properties",
                            logPath, node.nodeIndex()), RolePaths.KAFKA_BROKER_LOG4J)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(BrokerRole.KAFKA_CLASS_NAME)));
            }
            // Set up ZooKeeperStart command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("zk0", "zk1", "zk2")) {
                String nodeName = node.nodeName();
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        ZooKeeperRole.ZOOKEEPER_CLASS_NAME, false).toArray(new String[0])));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, ZooKeeperStart.createSetupPathsCommandLine()));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, ZooKeeperStart.createRunDaemonCommandLine()));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/zookeeper-%d.properties",
                            logPath, node.nodeIndex()), RolePaths.ZK_PROPERTIES)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/zookeeper-log4j-%d.properties",
                            logPath, node.nodeIndex()), RolePaths.ZK_LOG4J)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(ZooKeeperRole.ZOOKEEPER_CLASS_NAME)));
            }
            // Set up Trogdor coordinator command line responses.
            for (SoakNode node : miniCluster.cluster().nodes("trogdor0")) {
                String nodeName = node.nodeName();
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        COORDINATOR.className(), false).toArray(new String[0])));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, TrogdorStart.createSetupPathsCommandLine(COORDINATOR)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, TrogdorStart.runDaemonCommandLine(COORDINATOR, nodeName)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/trogdor-%s-%d.conf",
                            logPath, COORDINATOR.name(), node.nodeIndex()),
                        COORDINATOR.propertiesPath())));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/trogdor-%s-log4j-%d.properties",
                            logPath, COORDINATOR.name(), node.nodeIndex()),
                        COORDINATOR.log4jConfPath())));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(COORDINATOR.className())));
            }
            // Set up rsync command line response for Kafka src.
            for (SoakNode node : miniCluster.cluster().
                    nodes("broker0", "broker1", "broker2", "zk0", "zk1", "zk2", "trogdor0")) {
                String nodeName = node.nodeName();
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, RsyncSrcAction.setupDirectoryCommand()));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        miniCluster.cluster().kafkaPath() + "/", RolePaths.KAFKA_SRC + "/")));
            }
            // Set up TrogdorStart command line responses for agent nodes.
            for (SoakNode node : miniCluster.cluster().
                    nodes("broker0", "broker1", "broker2", "zk0", "zk1", "zk2")) {
                String nodeName = node.nodeName();
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        AGENT.className(), false).toArray(new String[0])));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, TrogdorStart.createSetupPathsCommandLine(AGENT)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, TrogdorStart.runDaemonCommandLine(AGENT, nodeName)));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/trogdor-%s-%d.conf",
                            logPath, AGENT.name(), node.nodeIndex()), AGENT.propertiesPath())));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, miniCluster.rsyncToCommandLine(nodeName,
                        String.format("%s/trogdor-%s-log4j-%d.properties",
                            logPath, AGENT.name(), node.nodeIndex()), AGENT.log4jConfPath())));
                cloud.addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.
                        checkJavaProcessStatusArgs(AGENT.className())));
            }
            SoakSetup.soakSetup(miniCluster.cluster(), miniCluster.soakEnvironment());
        }
    }
}
