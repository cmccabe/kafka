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

import org.apache.kafka.soak.cloud.Ec2Cloud;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.role.ActionScheduler;
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.role.RoleDependencies;

import java.io.File;

/**
 * The "soak setup" command.
 */
public final class SoakSetup {
    static void run(String clusterPath, SoakEnvironment env) throws Throwable {
        SoakClusterSpec clusterSpec =
            SoakTool.JSON_SERDE.readValue(new File(clusterPath), SoakClusterSpec.class);
        int nodesSetup;
        try (Ec2Cloud cloud = new Ec2Cloud()) {
            try (SoakCluster cluster = new SoakCluster(clusterSpec, env, cloud)) {
                nodesSetup = soakSetup(cluster,  env);
            }
        }
        env.clusterLog().printf("*** Setup %d node(s)\n", nodesSetup);
        System.exit(0);
    }

    static int soakSetup(final SoakCluster cluster, SoakEnvironment env) throws Throwable {
        ActionScheduler.Builder builder = new ActionScheduler.Builder(cluster).
            dependencyHandler(RoleDependencies.DEPENDENCY_HANDLER).
            actionFilter(env.actionFilter());
        for (SoakNode node : cluster.nodes().values()) {
            for (Role role : node.spec().roles()) {
                role.setup(builder, node.nodeName());
            }
        }
        try (final ActionScheduler scheduler = builder.build()) {
            scheduler.await(env.timeoutMs());
        }
        return cluster.nodes().size();
    }
}
