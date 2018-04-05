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

import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An action which the soak tool can execute.
 */
public abstract class Action {
    /**
     * The ID of this action.
     */
    private final ActionId id;

    /**
     * Action IDs which must be run before this action, if they are to run.
     */
    private final Set<ActionId> comesAfter;

    /**
     * Action IDs which are run in whenever we run this action.
     */
    private final Set<ActionId> implies;

    public Action(ActionId id, ActionId[] comesAfter, ActionId[] implies) {
        this.id = id;
        this.comesAfter = Collections.
            unmodifiableSet(new HashSet<>(Arrays.asList(comesAfter)));
        this.implies = Collections.
            unmodifiableSet(new HashSet<>(Arrays.asList(implies)));
    }

    /**
     * Validate that the action can be run.
     * This happens before we start running any actions.
     */
    public void validate(SoakCluster cluster, SoakNode node) throws Throwable {}

    /**
     * Run the aciton.
     */
    public void call(SoakCluster cluster, SoakNode node) throws Throwable {}

    public ActionId id() {
        return id;
    }

    public Set<ActionId> comesAfter() {
        return comesAfter;
    }

    public Set<ActionId> implies() {
        return implies;
    }

    @Override
    public final int hashCode() {
        return id.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action other = (Action) o;
        return id.equals(other.id);
    }

    @Override
    public final String toString() {
        return id.toString();
    }
}
