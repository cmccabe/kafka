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
 * A Requirement which an Action can have.
 */
public final class Requirement {
    private final Dependency dependency;
    private final boolean hard;

    public static Requirement fromString(String str) {
        boolean hard = true;
        if (str.startsWith("?")) {
            hard = false;
            str = str.substring(1);
        }
        Dependency dependency = Dependency.fromString(str);
        return new Requirement(dependency, hard);
    }

    public Requirement(Dependency dependency, boolean hard) {
        this.dependency = dependency;
        this.hard = hard;
    }

    public Dependency dependency() {
        return dependency;
    }

    public boolean hard() {
        return hard;
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
        if (!hard) {
            bld.append("?");
        }
        bld.append(dependency.toString());
        return bld.toString();
    }
}
