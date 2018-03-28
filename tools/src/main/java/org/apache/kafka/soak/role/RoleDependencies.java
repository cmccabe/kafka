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

import org.apache.kafka.soak.role.ActionScheduler.UnresolvedDependencyHandler;
import org.apache.kafka.soak.role.ActionScheduler.UnsatisfiedDependencyException;

import java.util.Collection;

public final class RoleDependencies {
    public static final String LINUX_SETUP = "linuxSetup";
    public static final String RSYNC_SRC = "rsyncSrc";
    public static final String SAVE_LOGS = "saveLogs";

    public static final String BROKER_START = "brokerStart";
    public static final String BROKER_STATUS = "brokerStatus";
    public static final String BROKER_STOP = "brokerStop";

    public static final String ZOOKEEPER_START = "zooKeeperStart";
    public static final String ZOOKEEPER_STATUS = "zooKeeperStatus";
    public static final String ZOOKEEPER_STOP = "zooKeeperStop";

    public static final String START_SUFFIX = "Start";
    public static final String STATUS_SUFFIX = "Status";
    public static final String STOP_SUFFIX = "Stop";

    public static final String TROGDOR_TASKS_START = "trogdorTasksStart";
    public static final String TROGDOR_TASKS_STATUS = "trogdorTasksStatus";
    public static final String TROGDOR_TASKS_STOP = "trogdorTasksStop";

    public static final SoakUnresolvedDependencyHandler DEPENDENCY_HANDLER =
        new SoakUnresolvedDependencyHandler();

    public static class SoakUnresolvedDependencyHandler implements UnresolvedDependencyHandler {
        @Override
        public Action resolve(Dependency dep, Collection<String> clusterNodeNames)
                throws UnsatisfiedDependencyException {
            if (dep.scope().equals("all")) {
                throw new UnsatisfiedDependencyException(dep);
            }
            if (dep.id().equals(RSYNC_SRC)) {
                return new RsyncSrcAction(dep.scope());
            }
            throw new UnsatisfiedDependencyException(dep);
        }
    }
};
