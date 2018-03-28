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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cloud.Ec2Cloud;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.cluster.SoakNodeSpec;
import org.apache.kafka.soak.common.SoakLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The "soak down" command.
 */
public final class SoakDown {
    private static final Logger log = LoggerFactory.getLogger(SoakDown.class);

    static void run(String clusterPath, SoakEnvironment env) throws Throwable {
        SoakClusterSpec clusterSpec =
            SoakTool.JSON_SERDE.readValue(new File(clusterPath), SoakClusterSpec.class);
        int nodesRemoved = 0;
        try (Ec2Cloud cloud = new Ec2Cloud()) {
            try (SoakCluster cluster = new SoakCluster(clusterSpec, env, cloud)) {
                nodesRemoved = soakDown(cluster, env);
            }
        }
        env.clusterLog().printf("*** Brought down %d node(s)\n", nodesRemoved);
        // Delete the cluster file.
        Files.delete(Paths.get(clusterPath));
        System.exit(0);
    }

    static int soakDown(final SoakCluster cluster, SoakEnvironment env) throws Throwable {
        // Make AWS call to bring down all nodes
        // Any node that we fail to bring down gets added to newCluster
        List<SoakNode> nodes = new ArrayList<>();
        List<String> instanceIds = new ArrayList<>();
        for (SoakNode node : cluster.nodes().values()) {
            if (!node.spec().instanceId().isEmpty()) {
                nodes.add(node);
                instanceIds.add(node.spec().instanceId());
            }
        }
        if (!instanceIds.isEmpty()) {
            try {
                cluster.cloud().terminateInstances(instanceIds.toArray(new String[0]));
                for (SoakNode node : nodes) {
                    String instanceId = node.spec().instanceId();
                    node.setSpec(new SoakNodeSpec.Builder(node.spec()).instanceId("").build());
                    SoakLog.printToAll(String.format("*** Took down %s\n", instanceId),
                        node.log(), cluster.clusterLog());
                }
            } catch (Throwable t) {
                log.error("Error bringing down nodes {}",
                    Utils.join(instanceIds, ", "), t);
                throw t;
            }
        }
        return nodes.size();
    }
};
