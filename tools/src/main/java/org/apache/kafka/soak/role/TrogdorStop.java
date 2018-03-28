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
import org.apache.kafka.soak.common.SoakUtil;

import static org.apache.kafka.soak.role.RoleDependencies.BROKER_STOP;
import static org.apache.kafka.soak.role.RoleDependencies.TROGDOR_TASKS_STOP;

/**
 * Stop Trogdor.
 */
public final class TrogdorStop extends Action {
    private final TrogdorDaemonType daemonType;

    public TrogdorStop(String nodeName, TrogdorDaemonType daemonType) {
        super(daemonType.stop(),
            nodeName,
            new String[] {
                BROKER_STOP + ":all",
                TROGDOR_TASKS_STOP + ":all"
            },
            new String[] {daemonType.stop()});
        this.daemonType = daemonType;
    }

    @Override
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
        SoakUtil.killJavaProcess(cluster, node, daemonType.className(), false);
    }
}
