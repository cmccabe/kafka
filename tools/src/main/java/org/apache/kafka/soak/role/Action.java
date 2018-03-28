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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.tool.SoakTool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An action which the soak tool can execute.
 */
public abstract class Action {
    /**
     * The name of this action.
     */
    private final String actionName;

    /**
     * The node on which this action will be run.
     */
    private final String nodeName;

    /**
     * The dependencies that must have been run before this action can be run.
     */
    private final Set<Requirement> requirements;

    /**
     * The IDs of dependencies which this action provides.
     */
    private final Set<String> provides;

    /**
     * Constructs an Action.
     *
     * @param actionName    The name of the action.
     * @param nodeName      The node on which the aciton will execute.
     * @param requires      The requirements of the action, in string format.
     * @param provides      The IDs of requirements which this action provides.
     */
    public Action(String actionName, String nodeName, String[] requires, String[] provides) {
        this.actionName = actionName;
        this.nodeName = nodeName;

        HashSet<Requirement> reqs = new HashSet<>();
        // soft requirements start with a ?
        for (String depString : requires) {
            reqs.add(Requirement.fromString(depString));
        }
        this.requirements = Collections.unmodifiableSet(reqs);
        this.provides = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(provides)));
    }

    public abstract void call(SoakCluster cluster, SoakNode node) throws Throwable;

    @Override
    public final int hashCode() {
        return (17 * actionName.hashCode()) ^ nodeName.hashCode();
    }

    public String actionName() {
        return actionName;
    }

    public String nodeName() {
        return nodeName;
    }

    public Set<Requirement> requirements() {
        return requirements;
    }

    public Set<String> provides() {
        return provides;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action other = (Action) o;
        return actionName.equals(other.actionName) && nodeName.equals(other.nodeName);
    }

    public final String toJson() {
        try {
            return SoakTool.JSON_SERDE.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final String toString() {
        return actionName + ":" + nodeName;
    }
}
