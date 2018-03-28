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

import org.apache.kafka.soak.cluster.MiniSoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.cluster.SoakNodeSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;

public class SoakDownTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testSoakDown() throws Throwable {
        MiniSoakCluster.Builder builder = new MiniSoakCluster.Builder();
        builder.addNodeWithInstanceId("node0", new SoakNodeSpec.Builder().build());
        builder.addNodeWithInstanceId("node1", new SoakNodeSpec.Builder().build());
        builder.addNodeWithInstanceId("node2", new SoakNodeSpec.Builder().build());
        try (MiniSoakCluster miniCluster = builder.build()) {
            assertEquals(miniCluster.cluster().nodes().size(), miniCluster.cloud().numInstances());
            SoakDown.soakDown(miniCluster.cluster(), miniCluster.soakEnvironment());
            for (SoakNode node : miniCluster.cluster().nodes().values()) {
                assertEquals("", node.spec().instanceId());
            }
            assertEquals(0, miniCluster.cloud().numInstances());
        }
    }
};
