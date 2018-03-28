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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SoakClusterSpec {
    private final String soakId;
    private final Map<String, SoakNodeSpec> nodes;
    private final Map<String, String> defaults;
    private final List<String> provided;

    @JsonCreator
    public SoakClusterSpec(@JsonProperty("soakId") String soakId,
                           @JsonProperty("nodes") Map<String, SoakNodeSpec> nodes,
                           @JsonProperty("defaults") Map<String, String> defaults,
                           @JsonProperty("provided") List<String> provided) {
        this.soakId = soakId == null ? "" : soakId;
        this.nodes = Collections.unmodifiableMap(
            (nodes == null) ? new TreeMap<String, SoakNodeSpec>() : new TreeMap<>(nodes));
        this.defaults = Collections.unmodifiableMap(
            (defaults == null) ? new TreeMap<String, String>() : new TreeMap<>(defaults));
        this.provided = Collections.unmodifiableList(
            (provided == null) ? new ArrayList<String>() : new ArrayList<>(provided));
    }

    @JsonProperty
    public String soakId() {
        return soakId;
    }

    @JsonProperty
    public Map<String, SoakNodeSpec> nodes() {
        return nodes;
    }

    @JsonProperty
    public Map<String, String> defaults() {
        return defaults;
    }

    @JsonProperty
    public List<String> provided() {
        return provided;
    }
}
