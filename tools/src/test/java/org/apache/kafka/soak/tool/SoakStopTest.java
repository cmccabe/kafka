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

import org.apache.kafka.soak.cloud.MockCommandResponse;
import org.apache.kafka.soak.cluster.MiniSoakCluster;
import org.apache.kafka.soak.cluster.TestFixture;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.role.BrokerRole;
import org.apache.kafka.soak.role.TrogdorDaemonType;
import org.apache.kafka.soak.role.ZooKeeperRole;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;

public class SoakStopTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testSoakStop() throws Throwable {
        try (MiniSoakCluster miniCluster = TestFixture.createRoleTestCluster()) {
            for (String nodeName : new String[] {"broker0", "broker1", "broker2"}) {
                miniCluster.cloud().addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        BrokerRole.KAFKA_CLASS_NAME, true).toArray(new String[0])));
                miniCluster.cloud().addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        TrogdorDaemonType.AGENT.className(), false).toArray(new String[0])));
            }
            for (String nodeName : new String[] {"zk0", "zk1", "zk2"}) {
                miniCluster.cloud().addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        ZooKeeperRole.ZOOKEEPER_CLASS_NAME, false).toArray(new String[0])));
                miniCluster.cloud().addCommandResponse(nodeName,
                    new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                        TrogdorDaemonType.AGENT.className(), false).toArray(new String[0])));
            }
            miniCluster.cloud().addCommandResponse("trogdor0",
                new MockCommandResponse(0, SoakUtil.killJavaProcessArgs(
                    TrogdorDaemonType.COORDINATOR.className(), false).toArray(new String[0])));
            SoakStop.soakStop(miniCluster.cluster(), miniCluster.soakEnvironment());
            for (String nodeName : new String[] {"broker0", "broker1", "broker2"}) {
                assertEquals(2, miniCluster.cloud().commandResponsesRemoved(nodeName));
            }
            for (String nodeName : new String[] {"zk0", "zk1", "zk2"}) {
                assertEquals(2, miniCluster.cloud().commandResponsesRemoved(nodeName));
            }
            assertEquals(1, miniCluster.cloud().commandResponsesRemoved("trogdor0"));
        }
    }
}
