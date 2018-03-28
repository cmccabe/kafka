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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.role.Role;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A node in the soak cluster.
 */
public class SoakNodeSpec {
    private final String instanceId;
    private final String privateDns;
    private final String publicDns;
    private final List<Role> roles;
    private final Map<String, String> conf;

    public static class Builder {
        private String instanceId;
        private String privateDns;
        private String publicDns;
        private List<Role> roles;
        private Map<String, String> conf;

        public Builder() {
            this.instanceId = "";
            this.privateDns = "";
            this.publicDns = "";
            this.roles = new ArrayList<>();
            this.conf = new TreeMap<>();
        }

        public Builder(SoakNodeSpec node) {
            this.instanceId = node.instanceId;
            this.privateDns = node.privateDns;
            this.publicDns = node.publicDns;
            this.roles = node.roles;
            this.conf = node.conf;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder privateDns(String privateDns) {
            this.privateDns = privateDns;
            return this;
        }

        public Builder publicDns(String publicDns) {
            this.publicDns = publicDns;
            return this;
        }

        public Builder roles(List<Role> roles) {
            this.roles = roles;
            return this;
        }

        public Builder conf(TreeMap<String, String> conf) {
            this.conf = conf;
            return this;
        }

        public SoakNodeSpec build() {
            return new SoakNodeSpec(instanceId, privateDns, publicDns, roles, conf);
        }
    }

    @JsonCreator
    public SoakNodeSpec(@JsonProperty("instanceId") String instanceId,
                        @JsonProperty("privateDns") String privateDns,
                        @JsonProperty("publicDns") String publicDns,
                        @JsonProperty("roles") List<Role> roles,
                        @JsonProperty("conf") Map<String, String> conf) {
        this.instanceId = instanceId == null ? "" : instanceId;
        this.privateDns = privateDns == null ? "" : privateDns;
        this.publicDns = publicDns == null ? "" : publicDns;
        this.roles = Collections.unmodifiableList(
            roles == null ? new ArrayList<Role>(0) : new ArrayList<>(roles));
        this.conf = Collections.unmodifiableMap(
            conf == null ? new TreeMap<String, String>() : new TreeMap<>(conf));
    }

    @JsonProperty
    public String instanceId() {
        return instanceId;
    }

    @JsonProperty
    public String privateDns() {
        return privateDns;
    }

    @JsonProperty
    public String publicDns() {
        return publicDns;
    }

    public String dns(boolean internal) {
        return internal ? privateDns : publicDns;
    }

    @JsonProperty
    public List<Role> roles() {
        return roles;
    }

    @JsonProperty
    public Map<String, String> conf() {
        return conf;
    }
}
