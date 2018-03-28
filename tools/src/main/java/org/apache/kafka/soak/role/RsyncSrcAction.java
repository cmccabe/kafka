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

package org.apache.kafka.soak.role;

import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;

import static org.apache.kafka.soak.role.RoleDependencies.LINUX_SETUP;
import static org.apache.kafka.soak.role.RoleDependencies.RSYNC_SRC;

/**
 * Rsync the Kafka source directory to the cluster node.
 */
public final class RsyncSrcAction extends Action {
    public RsyncSrcAction(String nodeName) {
        super(RSYNC_SRC,
            nodeName,
            new String[] {"?" +LINUX_SETUP + ":" + nodeName},
            new String[] {RSYNC_SRC});
    }

    @Override
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        cluster.cloud().remoteCommand(node).args(setupDirectoryCommand()).mustRun();
        cluster.cloud().remoteCommand(node).
            syncTo(cluster.kafkaPath() + "/", RolePaths.KAFKA_SRC + "/").
            mustRun();
    }

    public static String[] setupDirectoryCommand() {
        return new String[] {"sudo", "mkdir", "-p", RolePaths.KAFKA_SRC, "&&",
            "sudo", "chown", "-R", "`whoami`", RolePaths.KAFKA_SRC};
    }
}
