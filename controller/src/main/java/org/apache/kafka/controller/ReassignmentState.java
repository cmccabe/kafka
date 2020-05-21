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

package org.apache.kafka.controller;

public enum ReassignmentState {
    NONE((byte) 0),
    ADDING((byte) 1),
    REMOVING((byte) 2);

    private final byte val;

    private final static ReassignmentState[] VALUES = values();

    static ReassignmentState fromVal(byte val) {
        for (ReassignmentState state : VALUES) {
            if (state.val == val) {
                return state;
            }
        }
        throw new RuntimeException("Unknown ReassignmentState " + (int) val);
    }

    ReassignmentState(byte val) {
        this.val = val;
    }

    public byte val() {
        return val;
    }
}
