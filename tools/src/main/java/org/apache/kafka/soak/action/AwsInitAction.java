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

package org.apache.kafka.soak.action;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.cluster.SoakNodeSpec;
import org.apache.kafka.soak.common.SoakConfig;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.AwsNodeRole;
import org.apache.kafka.soak.tool.SoakReturnCode;
import org.apache.kafka.soak.tool.SoakShutdownHook;
import org.apache.kafka.soak.tool.SoakTool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Initiates a new AWS node.
 */
public final class AwsInitAction extends Action {
    private static final int DNS_POLL_DELAY_MS = 200;

    private static final int SSH_POLL_DELAY_MS = 200;

    public static String TYPE = "awsInit";

    private final AwsNodeRole role;

    public AwsInitAction(String scope, AwsNodeRole role) {
        super(new ActionId(TYPE, scope),
            new ActionId[] {},
            new ActionId[] {});
        this.role = role;
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        SoakClusterSpec clusterSpec =
            SoakTool.JSON_SERDE.readValue(new File(cluster.env().testSpecPath()),
                SoakClusterSpec.class);
        if (new File(cluster.env().clusterPath()).exists()) {
            throw new RuntimeException("Output cluster path " + cluster.env().clusterPath() +
                " already exists.");
        }

        // Create a new instance.
        TreeMap<String, String> params = new TreeMap<>();
        params.put(SoakConfig.IMAGE_ID, role.imageId());
        params.put(SoakConfig.INSTANCE_TYPE, role.instanceType());
        String instanceId = cluster.cloud().runInstance(params);

        // Make sure that we don't leak an AWS instance if we shut down unexpectedly.
        cluster.shutdownManager().addHookIfMissing(new DestroyAwsInstancesShutdownHook(cluster));

        node.setSpec(new SoakNodeSpec.Builder().
            instanceId(instanceId).
            build());

        // Wait for the DNS to be set up.
        do {
            if (DNS_POLL_DELAY_MS > 0) {
                Thread.sleep(DNS_POLL_DELAY_MS);
            }
        } while (!checkInstanceDns(cluster, node));

        // Wait for the SSH to work
        do {
            if (SSH_POLL_DELAY_MS > 0) {
                Thread.sleep(SSH_POLL_DELAY_MS);
            }
        } while (!checkInstanceSsh(cluster, node));

        // Write out the new cluster file.
        SoakTool.JSON_SERDE.writeValue(new File(cluster.env().clusterPath()), cluster.toSpec());
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

    /**
     * Destroys an AWS instance on shutdown.
     */
    public final class DestroyAwsInstancesShutdownHook extends SoakShutdownHook {
        private final SoakCluster cluster;

        DestroyAwsInstancesShutdownHook(SoakCluster cluster) {
            super("DestroyAwsInstancesShutdownHook");
            this.cluster = cluster;
        }

        @Override
        public void run(SoakReturnCode returnCode) throws Throwable {
            if (returnCode == SoakReturnCode.SUCCESS) {
                String path = cluster.env().clusterPath();
                try {
                    SoakTool.JSON_SERDE.writeValue(new File(path), cluster.toSpec());
                    cluster.clusterLog().info("*** Wrote new cluster file to " + path);
                } catch (Throwable e) {
                    cluster.clusterLog().error("*** Failed to write cluster file to " + path, e);
                    terminateInstances();
                    throw e;
                }
            } else {
                terminateInstances();
            }
        }

        private synchronized void terminateInstances() throws Throwable {
            List<String> instanceIds = new ArrayList<>();
            for (SoakNode node : cluster.nodes().values()) {
                String instanceId = node.spec().instanceId();
                if (!instanceId.isEmpty()) {
                    instanceIds.add(instanceId);
                }
            }
            if (!instanceIds.isEmpty()) {
                cluster.cloud().terminateInstances(instanceIds.toArray(new String[0]));
            }
            cluster.clusterLog().info("*** Terminated instance IDs " +
                Utils.join(instanceIds, ", "));
            for (SoakNode node : cluster.nodes().values()) {
                if (instanceIds.contains(node.spec().instanceId())) {
                    node.log().info("*** Terminated instance " + node.spec().instanceId());
                }
            }
        }
    }
}
