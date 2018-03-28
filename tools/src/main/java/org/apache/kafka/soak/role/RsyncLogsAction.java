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

import static org.apache.kafka.soak.role.RoleDependencies.BROKER_STOP;
import static org.apache.kafka.soak.role.RoleDependencies.SAVE_LOGS;
import static org.apache.kafka.soak.role.RoleDependencies.ZOOKEEPER_STOP;

/**
 * Rsync the Kafka source directory to the cluster node.
 */
public final class RsyncLogsAction extends Action {
    public static final String RSYNC_LOGS = "rsyncLogs";

    public RsyncLogsAction(String nodeName) {
        super(RSYNC_LOGS,
            nodeName,
            new String[] {
                "?" + BROKER_STOP + ":" + nodeName,
                "?" + TrogdorDaemonType.AGENT.stop() + ":" + nodeName,
                "?" + TrogdorDaemonType.COORDINATOR.stop() + ":" + nodeName,
                "?" + ZOOKEEPER_STOP + ":" + nodeName
            },
            new String[] {SAVE_LOGS});
    }

    @Override
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        cluster.cloud().remoteCommand(node).
            syncFrom(RolePaths.LOGS_ROOT + "/", cluster.logPath() + "logs/").
            mustRun();
    }
}
