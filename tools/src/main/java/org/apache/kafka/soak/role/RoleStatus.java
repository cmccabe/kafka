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
 * The status of a role.
 *
 * The status includes the state, and a textual description.
 */
public class RoleStatus {
    private final RoleState state;
    private final String description;

    public RoleStatus(RoleState state, String description) {
        this.state = state;
        this.description = description;
    }

    public RoleState state() {
        return state;
    }

    public String description() {
        return description;
    }
};
