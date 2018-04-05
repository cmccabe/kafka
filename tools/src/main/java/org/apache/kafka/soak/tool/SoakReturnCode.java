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

package org.apache.kafka.soak.tool;

/**
 * Codes which the soak tool returns on the command line.
 */
public enum SoakReturnCode {
    /**
     * The soak tool succeeded.
     */
    SUCCESS(0),

    /**
     * A task is in progress.
     */
    IN_PROGRESS(100),

    /**
     * The soak cluster failed.
     */
    CLUSTER_FAILED(101),

    /**
     * The soak tool failed.
     */
    TOOL_FAILED(102);

    private final int code;

    SoakReturnCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static SoakReturnCode worstOf(SoakReturnCode a, SoakReturnCode b) {
        return (a.ordinal() < b.ordinal()) ? b : a;
    }
};
