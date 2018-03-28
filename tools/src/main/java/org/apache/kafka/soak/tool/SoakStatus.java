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
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.ActionScheduler;
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.role.RoleDependencies;
import org.apache.kafka.soak.role.RoleState;
import org.apache.kafka.soak.role.RoleStatus;
import org.apache.kafka.soak.role.RoleStatusCollector;

import java.io.File;

/**
 * The "soak status" command.
 */
public final class SoakStatus {
    static void run(String clusterPath, SoakEnvironment env) throws Throwable {
        SoakClusterSpec clusterSpec =
            SoakTool.JSON_SERDE.readValue(new File(clusterPath), SoakClusterSpec.class);
        int exitStatus;
        try (Ec2Cloud cloud = new Ec2Cloud()) {
            try (SoakCluster cluster = new SoakCluster(clusterSpec, env, cloud)) {
                exitStatus = getStatuses(cluster, env);
            }
        }
        System.exit(exitStatus);
    }

    private static class SoakStatusCollector implements RoleStatusCollector {
        private final SoakCluster cluster;
        private RoleState state = RoleState.SUCCESS;

        SoakStatusCollector(SoakCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public synchronized void collect(String nodeName, RoleStatus status) {
            state = state.min(status.state());
            if (!status.description().isEmpty()) {
                SoakLog.printToAll(String.format("*** %s: %s [%s]\n",
                        nodeName, status.description(), status.state()),
                    cluster.nodes().get(nodeName).log(),
                    cluster.clusterLog());
            }
        }

        synchronized RoleState state() {
            return state;
        }
    }

    static int getStatuses(SoakCluster cluster, SoakEnvironment env)
            throws Throwable {
        final SoakStatusCollector collector = new SoakStatusCollector(cluster);
        ActionScheduler.Builder builder = new ActionScheduler.Builder(cluster).
            dependencyHandler(RoleDependencies.DEPENDENCY_HANDLER).
            actionFilter(env.actionFilter());
        for (SoakNode node : cluster.nodes().values()) {
            for (Role role : node.spec().roles()) {
                role.status(builder, node.nodeName(), collector);
            }
        }
        try (final ActionScheduler scheduler = builder.build()) {
            scheduler.await(env.timeoutMs());
        }
        switch (collector.state()) {
            case FAILED:
                return 1;
            case WAITING:
                return 2;
            default:
                return 0;
        }
    }
};
