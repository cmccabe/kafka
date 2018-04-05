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

import java.util.Objects;

/**
 * An action ID.
 */
public final class ActionId {
    public static final String SCOPE_ALL = "all";

    public static ActionId parse(String value) {
        int index = value.indexOf(':');
        if (index < 0) {
            return new ActionId(value, SCOPE_ALL);
        }
        return new ActionId(value.substring(0, index), value.substring(index + 1));
    }

    /**
     * The name of this action.
     */
    private final String type;

    /**
     * The node on which this action will be run.
     */
    private final String scope;

    public ActionId(String type, String scope) {
        this.type = type;
        this.scope = scope;
    }

    public String type() {
        return type;
    }

    public String scope() {
        return scope;
    }

    public boolean hasGlobalScope() {
        return scope.equals(SCOPE_ALL);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, scope);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActionId other = (ActionId) o;
        return type.equals(other.type) && scope.equals(other.scope);
    }

    @Override
    public final String toString() {
        return type + ":" + scope;
    }
}
