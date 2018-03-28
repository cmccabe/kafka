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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A role which a particular soak cluster node can have.
 *
 * A cluster node may have multiple roles.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY,
    property = "class")
public interface Role {
    /**
     * Create the setup actions for this node.
     *
     * @param bld           The scheduler builder.
     * @param nodeName      The name of this node.
     */
    void setup(ActionScheduler.Builder bld, String nodeName);

    /**
     * Create the status collection actions for this node.
     *
     * @param bld               The scheduler builder.
     * @param nodeName          The name of this node.
     * @param statusCollector   Collects status results.
     */
    void status(ActionScheduler.Builder bld, String nodeName,
                RoleStatusCollector statusCollector);

    /**
     * Create the stop actions for this node.
     *
     * @param bld               The scheduler builder.
     * @param nodeName          The name of this node.
     */
    void stop(ActionScheduler.Builder bld, String nodeName);
};
