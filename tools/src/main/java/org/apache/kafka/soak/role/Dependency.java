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

/**
 * A dependency which an Action can have.
 */
public final class Dependency {
    private final String id;
    private final String scope;

    public static void validateId(String depId) {
        if (depId.contains(":") || depId.startsWith("?")) {
            throw new RuntimeException("Invalid dependency ID " + depId + ".  Provided " +
                "dependency IDs cannot contain colons or question marks.");
        }
    }

    public static Dependency fromString(String str) {
        if (str.contains("?")) {
            throw new RuntimeException("Invalid dependency string format: found ? " +
                    "in " + str);
        }
        int colon = str.indexOf(':');
        if (colon == -1) {
            throw new RuntimeException("Invalid dependency string format: unable to " +
                    "find colon in " + str);
        }
        String id = str.substring(0, colon);
        String scope = str.substring(colon + 1);
        return new Dependency(id, scope);
    }

    public Dependency(String id, String scope) {
        this.id = id;
        this.scope = scope;
    }

    public String id() {
        return id;
    }

    public String scope() {
        return scope;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Dependency)) {
            return false;
        }
        return this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append(id).append(":").append(scope);
        return bld.toString();
    }
}
