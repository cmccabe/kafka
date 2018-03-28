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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.role.TrogdorTasksRole.CoordinatorFunction;
import org.apache.kafka.soak.tool.SoakTool;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.TaskDone;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.kafka.soak.role.RoleDependencies.TROGDOR_TASKS_STATUS;

public class TrogdorTasksStatus extends Action  {
    private final Collection<String> taskIds;
    private final RoleStatusCollector statusCollector;

    public TrogdorTasksStatus(String nodeName, Collection<String> taskIds,
                              RoleStatusCollector statusCollector) {
        super(TROGDOR_TASKS_STATUS,
            nodeName,
            new String[] {
                TrogdorDaemonType.COORDINATOR.status() + ":all"
            },
            new String[] {
                TROGDOR_TASKS_STATUS
            });
        this.taskIds = Collections.unmodifiableCollection(taskIds == null ?
            new ArrayList<String>() : taskIds);
        this.statusCollector = statusCollector;
    }

    @Override
    public void call(final SoakCluster cluster, SoakNode node) throws Throwable {
        try {
            TasksResponse response = TrogdorTasksRole.invokeCoordinator(
                cluster, node, new CoordinatorFunction<TasksResponse>() {
                    @Override
                    public TasksResponse apply(CoordinatorClient coordinatorClient) throws Exception {
                        TasksResponse response = coordinatorClient.tasks(TasksRequest.ALL);
                        if (response == null) {
                            throw new RuntimeException("Invalid null TaskResponse");
                        }
                        return response;
                    }
                });
            RoleState returnState = RoleState.SUCCESS;
            ObjectNode results = new ObjectNode(JsonNodeFactory.instance);
            for (String taskId : taskIds) {
                TaskState state = response.tasks().get(taskId);
                if (state == null) {
                    returnState = returnState.min(RoleState.FAILED);
                    results.set(taskId, new TextNode("NOT FOUND"));
                } else if (state instanceof TaskDone) {
                    TaskDone doneState = (TaskDone) state;
                    if (doneState.error().isEmpty()) {
                        returnState = returnState.min(RoleState.SUCCESS);
                    } else {
                        returnState = returnState.min(RoleState.WAITING);
                    }
                    results.set(taskId, state.status());
                } else {
                    returnState = returnState.min(RoleState.WAITING);
                    results.set(taskId, state.status());
                }
            }
            RoleStatus roleStatus = new RoleStatus(returnState,
                SoakTool.JSON_SERDE.writeValueAsString(results));
            statusCollector.collect(node.nodeName(), roleStatus);
        } catch (Throwable t) {
            statusCollector.collect(node.nodeName(), new RoleStatus(
                RoleState.FAILED, "Error getting status: " + Utils.fullStackTrace(t)));
        }
    }
};
