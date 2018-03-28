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
 * The state of a role.
 */
public enum RoleState {
    /**
     * The role has failed.
     */
    FAILED,

    /**
     * We are waiting for the role to fail or succeed.
     */
    WAITING,

    /**
     * The role has succeeded.
     */
    SUCCESS;

    /**
     * Return the minimum state.
     *
     * @param other     The other state to compare.
     * @return          The minimum state.
     */
    public RoleState min(RoleState other) {
        return RoleState.values()[Math.min(ordinal(), other.ordinal())];
    }
};
