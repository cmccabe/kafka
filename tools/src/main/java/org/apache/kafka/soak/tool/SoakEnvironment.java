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

import org.apache.kafka.soak.common.NullOutputStream;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.ActionScheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public final class SoakEnvironment {
    public static class Builder {
        private SoakLog clusterLog = new SoakLog(SoakLog.CLUSTER, NullOutputStream.INSTANCE);
        private String rootPath = null;
        private String kafkaPath = null;
        private int timeoutSecs = 360;
        private String keyPair = "";
        private String securityGroup = "";
        private Pattern actionFilter = ActionScheduler.DEFAULT_ACTION_FILTER;

        public Builder() {
        }

        public Builder clusterLog(SoakLog clusterLog) {
            this.clusterLog = clusterLog;
            return this;
        }

        public SoakLog clusterLog() {
            return this.clusterLog;
        }

        public Builder rootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public String rootPath() {
            return this.rootPath;
        }

        public Builder kafkaPath(String kafkaPath) {
            this.kafkaPath = kafkaPath;
            return this;
        }

        public String kafkaPath() {
            return this.kafkaPath;
        }

        public Builder timeoutSecs(int timeoutSecs) {
            this.timeoutSecs = timeoutSecs;
            return this;
        }

        public int timeoutSecs() {
            return this.timeoutSecs;
        }

        public Builder keyPair(String keyPair) {
            this.keyPair = keyPair;
            return this;
        }

        public String keyPair() {
            return keyPair;
        }

        public Builder securityGroup(String securityGroup) {
            this.securityGroup = securityGroup;
            return this;
        }

        public String securityGroup() {
            return securityGroup;
        }

        public Builder actionFilter(Pattern actionFilter) {
            this.actionFilter = actionFilter;
            return this;
        }

        public Pattern actionFilter() {
            return actionFilter;
        }

        public SoakEnvironment build() throws IOException {
            if (rootPath == null) {
                throw new RuntimeException("Must initialize rootPath");
            }
            if (kafkaPath == null) {
                throw new RuntimeException("Must initialize KafkaPath");
            }
            return new SoakEnvironment(clusterLog, rootPath, kafkaPath, timeoutSecs,
                keyPair, securityGroup, actionFilter);
        }
    }

    private final SoakLog clusterLog;
    private final String rootPath;
    private final String kafkaPath;
    private final int timeoutSecs;
    private final String keyPair;
    private final String securityGroup;
    private final Pattern actionFilter;

    private SoakEnvironment(SoakLog clusterLog, String rootPath, String kafkaPath,
            int timeoutSecs, String keyPair, String securityGroup, Pattern actionFilter) {
        this.clusterLog = clusterLog;
        this.rootPath = rootPath;
        this.kafkaPath = kafkaPath;
        this.timeoutSecs = timeoutSecs;
        this.keyPair = keyPair;
        this.securityGroup = securityGroup;
        this.actionFilter = actionFilter;
    }

    public SoakLog clusterLog() {
        return clusterLog;
    }

    public String rootPath() {
        return rootPath;
    }

    public String kafkaPath() {
        return kafkaPath;
    }

    public int timeoutSecs() {
        return timeoutSecs;
    }

    public long timeoutMs() {
        return 1000L * timeoutSecs;
    }

    public String keyPair() {
        return keyPair;
    }

    public String securityGroup() {
        return securityGroup;
    }

    public Pattern actionFilter() {
        return actionFilter;
    }
};
