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

package org.apache.kafka.soak.cluster;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.common.SoakConfig;
import org.apache.kafka.soak.common.SoakLog;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a node in the soak cluster.
 */
public final class SoakNode implements AutoCloseable {
    /**
     * The index of this node in the cluster.  The node with the alphabetically
     * first name will be index 0, and so on.
     */
    private final int nodeIndex;

    /**
     * The soak cluster node name.
     */
    private final String nodeName;

    /**
     * The log for this node.
     */
    private final SoakLog soakLog;

    /**
     * The configuration for this node.
     *
     * This configuration map includes entries from the cluster defaults.
     */
    private final Map<String, String> conf;

    /**
     * The specification for this node.
     */
    private SoakNodeSpec spec;

    SoakNode(int nodeIndex, String nodeName, SoakLog soakLog, Map<String, String> conf,
             SoakNodeSpec spec) {
        this.nodeIndex = nodeIndex;
        this.nodeName = nodeName;
        this.soakLog = soakLog;
        this.conf = Collections.unmodifiableMap(conf);
        this.spec = spec;
    }

    public int nodeIndex() {
        return nodeIndex;
    }

    public String nodeName() {
        return nodeName;
    }

    public SoakLog log() {
        return soakLog;
    }

    public Map<String, String> conf() {
        return conf;
    }

    public boolean getBooleanConf(String configKey, boolean defaultValue) {
        String value = conf.get(configKey);
        if ((value == null) || value.isEmpty()) {
            return defaultValue;
        }
        return value.trim().equalsIgnoreCase("true");
    }

    public synchronized String dns() {
        boolean useInternalDns =
            getBooleanConf(SoakConfig.INTERNAL_DNS_KEY, SoakConfig.INTERNAL_DNS_DEFAULT);
        return spec.dns(useInternalDns);
    }

    public synchronized SoakNodeSpec spec() {
        return spec;
    }

    public synchronized void setSpec(SoakNodeSpec spec) {
        this.spec = spec;
    }

    @Override
    public void close() {
        Utils.closeQuietly(soakLog, "soakLog for " + nodeName);
    }
};
