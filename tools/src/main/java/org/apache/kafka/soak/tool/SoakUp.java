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
import org.apache.kafka.soak.cluster.SoakNodeSpec;
import org.apache.kafka.soak.common.SoakConfig;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.Action;
import org.apache.kafka.soak.role.ActionScheduler;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The "soak up" command.
 */
public final class SoakUp {
    private static final int DNS_POLL_DELAY_MS = 200;

    private static final int SSH_POLL_DELAY_MS = 200;

    static void run(String testSpecPath, final String outputPath, SoakEnvironment env)
            throws Throwable {
        SoakClusterSpec clusterSpec =
            SoakTool.JSON_SERDE.readValue(new File(testSpecPath), SoakClusterSpec.class);
        if (new File(outputPath).exists()) {
            throw new RuntimeException("Output cluster path " + outputPath + " already exists.");
        }
        try (final Ec2Cloud ec2Cloud = new Ec2Cloud(env.keyPair(), env.securityGroup())) {
            if (clusterSpec.soakId().isEmpty()) {
                String soakId = "soak-" + UUID.randomUUID().toString();
                env.clusterLog().printf("*** Generated random cluster ID: %s\n", soakId);
                clusterSpec = new SoakClusterSpec(soakId,
                    clusterSpec.nodes(),
                    clusterSpec.defaults(),
                    clusterSpec.provided());
            }
            try (final SoakCluster cluster = new SoakCluster(clusterSpec, env, ec2Cloud)) {
                Set<String> newInstanceIds = soakUp(cluster, env);
                boolean success = false;
                try {
                    if (newInstanceIds.size() == 0) {
                        throw new RuntimeException("All nodes are already up.");
                    }
                    SoakTool.JSON_SERDE.writeValue(new File(outputPath), cluster.toSpec());
                    env.clusterLog().printf("*** Brought up %d node(s)\n", newInstanceIds.size());
                    success = true;
                } finally {
                    if (!success) {
                        cluster.cloud().terminateInstances(newInstanceIds.toArray(new String[0]));
                    }
                }
            }
        }
        System.exit(0);
    }

    static Set<String> soakUp(SoakCluster cluster, SoakEnvironment env) throws Throwable {
        ActionScheduler.Builder builder = new ActionScheduler.Builder(cluster);
        ConcurrentSkipListSet<String> newInstanceIds = new ConcurrentSkipListSet<>();
        for (SoakNode node : cluster.nodes().values()) {
            if (node.spec().instanceId().isEmpty()) {
                builder.addAction(new RunInstance(node.nodeName(), newInstanceIds));
                builder.addAction(new WaitForDns(node.nodeName(), DNS_POLL_DELAY_MS));
                builder.addAction(new WaitForSsh(node.nodeName(), SSH_POLL_DELAY_MS));
            }
        }
        boolean success = false;
        try (ActionScheduler scheduler = builder.build()) {
            scheduler.await(env.timeoutMs());
            success = true;
        } finally {
            if (!success) {
                cluster.cloud().terminateInstances(newInstanceIds.toArray(new String[0]));
            }
        }
        return newInstanceIds;
    }

    private static class RunInstance extends Action {
        static final String NAME = "runInstance";
        private final ConcurrentSkipListSet<String> newInstanceIds;

        RunInstance(String nodeName, ConcurrentSkipListSet<String> newInstanceIds) {
            super(NAME, nodeName, new String[0], new String[] {NAME});
            this.newInstanceIds = newInstanceIds;
        }

        @Override
        public void call(SoakCluster cluster, SoakNode node) throws Throwable {
            TreeMap<String, String> params = new TreeMap<>();
            params.put(SoakConfig.IMAGE_ID, node.conf().get(SoakConfig.IMAGE_ID));
            params.put(SoakConfig.INSTANCE_TYPE, node.conf().get(SoakConfig.INSTANCE_TYPE));
            String instanceId = cluster.cloud().runInstance(params);
            newInstanceIds.add(instanceId);
            SoakNodeSpec newNodeSpec = new SoakNodeSpec.Builder(node.spec()).
                instanceId(instanceId).
                build();
            node.setSpec(newNodeSpec);
        }
    }

    private static class WaitForDns extends Action {
        static final String NAME = "waitForDns";
        private final long pollMs;

        WaitForDns(String nodeName, long pollMs) {
            super(NAME,
                nodeName,
                new String[] {
                    RunInstance.NAME + ":" + nodeName
                },
                new String[] {
                    NAME
                });
            this.pollMs = pollMs;
        }

        @Override
        public void call(SoakCluster cluster, SoakNode node) throws Throwable {
            do {
                if (pollMs > 0) {
                    Thread.sleep(pollMs);
                }
            } while (!checkInstanceDns(cluster, node));
        }

        private boolean checkInstanceDns(SoakCluster cluster, SoakNode node) throws Throwable {
            String instanceId = node.spec().instanceId();
            Map<String, String> result = cluster.cloud().describeInstance(instanceId);
            String privateDns = result.get(SoakConfig.PRIVATE_DNS);
            if (privateDns == null) {
                privateDns = "";
            }
            if (privateDns.isEmpty()) {
                node.log().printf("*** Waiting for private DNS name for %s...\n", instanceId);
                return false;
            }
            String publicDns = result.get(SoakConfig.PUBLIC_DNS);
            if (publicDns == null) {
                publicDns = "";
            }
            if (publicDns.isEmpty()) {
                node.log().printf("*** Waiting for public DNS name for %s...\n", instanceId);
                return false;
            }
            node.log().printf("*** Got privateDnsName = %s, publicDnsName = %s\n", privateDns, publicDns);
            node.setSpec(new SoakNodeSpec.Builder(node.spec()).
                publicDns(publicDns).
                privateDns(privateDns).
                build());
            return true;
        }
    }

    private static class WaitForSsh extends Action {
        static final String NAME = "waitForSsh";
        private final long pollMs;

        WaitForSsh(String nodeName, long pollMs) {
            super(NAME,
                nodeName,
                new String[] {
                    RunInstance.NAME  + ":" + nodeName,
                    WaitForDns.NAME + ":" + nodeName
                },
                new String[] {
                    NAME
                });
            this.pollMs = pollMs;
        }

        @Override
        public void call(SoakCluster cluster, SoakNode node) throws Throwable {
            do {
                if (pollMs > 0) {
                    Thread.sleep(pollMs);
                }
            } while (!checkInstanceSsh(cluster, node));
        }

        private boolean checkInstanceSsh(SoakCluster cluster, SoakNode node) throws Throwable {
            try {
                cluster.cloud().remoteCommand(node).args("-n", "--", "echo").mustRun();
            } catch (Exception e) {
                node.log().printf("*** Unable to ssh to %s: %s\n",
                    node.nodeName(), e.getMessage());
                return false;
            }
            SoakLog.printToAll(String.format("*** Successfully brought up %s\n", node.nodeName()),
                node.log(), cluster.clusterLog());
            return true;
        }
    }
}
