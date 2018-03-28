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
import org.apache.kafka.soak.cloud.SoakRemoteCommand;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.Action;
import org.apache.kafka.soak.role.ActionScheduler;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SoakClusterCommand {
    /**
     * Implement SoakTool ssh.
     *
     * @param clusterPath   The path to the cluster file.
     * @param env           The soak environment.
     * @param nodeArgs      The node name argument(s).
     * @param command       The command to run.
     * @throws Throwable
     */
    public static void run(String clusterPath, SoakEnvironment env,
            List<String> nodeArgs, List<String> command) throws Throwable {
        SoakClusterSpec clusterSpec = SoakTool.JSON_SERDE.
            readValue(new File(clusterPath), SoakClusterSpec.class);
        int retVal = 0;
        try (Ec2Cloud cloud = new Ec2Cloud()) {
            try (SoakCluster cluster = new SoakCluster(clusterSpec, env, cloud)) {
                Collection<String> nodeNames = cluster.getSoakNodesByNamesOrIndices(nodeArgs);
                if (nodeNames.isEmpty()) {
                    throw new RuntimeException("You must supply at least one node to ssh to.");
                } else if (nodeNames.size() == 1) {
                    retVal = sshToOne(cluster, nodeNames.iterator().next(), command);
                } else {
                    retVal = sshToMany(cluster, nodeNames, command, env);
                }
            }
        }
        System.exit(retVal);
    }

    /**
     * Ssh to a node interactively.
     *
     * @param cluster       The Kafka cluster
     * @param nodeName      The name of the node to ssh to.
     * @param command       The command to run.
     * @throws Throwable
     */
    public static int sshToOne(SoakCluster cluster, String nodeName, List<String> command)
            throws Throwable {
        SoakNode node = cluster.nodes().get(nodeName);
        List<String> commandLine = SoakRemoteCommand.createSshCommandPreamble(node);
        commandLine.add(node.dns());
        commandLine.addAll(command);
        node.log().printf("** %s: SSH %s\n", nodeName, SoakRemoteCommand.joinCommandLineArgs(commandLine));
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        return process.waitFor();
    }

    /**
     * Ssh to multiple nodes.
     *
     * @param cluster       The Kafka cluster
     * @param nodeNames     The names of the nodes to ssh to.
     * @param args          The command to run.
     * @param env           The environment.
     * @throws Throwable
     */
    public static int sshToMany(final SoakCluster cluster, Collection<String> nodeNames,
                                final List<String> args, final SoakEnvironment env) throws Throwable {
        ActionScheduler.Builder builder = new ActionScheduler.Builder(cluster);
        final AtomicBoolean failed = new AtomicBoolean(false);
        for (final String nodeName : nodeNames) {
            builder.addAction(new Action("ssh", nodeName, new String[0], new String[0]) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    int retVal = cluster.cloud().remoteCommand(node).argList(args).run();
                    if (retVal != 0) {
                        failed.set(true);
                    }
                    SoakLog.printToAll(String.format("** ssh %s %s completed with status %d\n",
                        nodeName, SoakRemoteCommand.joinCommandLineArgs(args), retVal),
                        node.log(), cluster.clusterLog());
                }
            });
        }
        try (ActionScheduler actionScheduler = builder.build()) {
            actionScheduler.await(env.timeoutMs());
        }
        return failed.get() ? 1 : 0;
    }
};
