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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.cloud.SoakRemoteCommand;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.trogdor.coordinator.Coordinator;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.TreeMap;

/**
 * A role which runs tasks inside Trogdor.
 */
public class TrogdorTasksRole implements Role {
    private final TreeMap<String, TaskSpec> taskSpecs;

    @JsonCreator
    public TrogdorTasksRole(@JsonProperty("taskSpecs") TreeMap<String, TaskSpec> taskSpecs) {
        this.taskSpecs = taskSpecs == null ? new TreeMap<String, TaskSpec>() : taskSpecs;
    }

    @JsonProperty
    public TreeMap<String, TaskSpec> taskSpecs() {
        return taskSpecs;
    }

    @Override
    public void setup(ActionScheduler.Builder bld, String nodeName) {
        bld.addAction(new TrogdorTasksStart(nodeName, taskSpecs));
    }

    @Override
    public void status(ActionScheduler.Builder bld, String nodeName,
                       RoleStatusCollector statusCollector) {
        bld.addAction(new TrogdorTasksStatus(nodeName, taskSpecs.keySet(), statusCollector));
    }

    @Override
    public void stop(ActionScheduler.Builder bld, String nodeName) {
        bld.addAction(new TrogdorTasksStop(nodeName, taskSpecs.keySet()));
    }

    interface CoordinatorFunction<T> {
        T apply(CoordinatorClient client) throws Exception;
    }

    /**
     * Create a coordinator client and open an ssh tunnel, so that we can invoke
     * the Trogdor coordinator.
     */
    static <T> T invokeCoordinator(final SoakCluster cluster, final SoakNode node,
                                   CoordinatorFunction<T> func) throws Exception {
        try (SoakRemoteCommand.Tunnel tunnel =
                 new SoakRemoteCommand.Tunnel(node, Coordinator.DEFAULT_PORT)) {
            CoordinatorClient coordinatorClient = new CoordinatorClient.Builder().
                maxTries(3).
                target("localhost", tunnel.localPort()).
                log(node.log()).
                build();
            return func.apply(coordinatorClient);
        }
    }
}
