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

package org.apache.kafka.image;

import org.apache.kafka.common.Node;

import java.util.Map;


/**
 * Represents a single node in the ClusterImage.
 *
 * This class is thread-safe.
 */
public final class NodeImage {
    private final int nodeId;
    private final String rack;
    private final Map<String, Node> endpoints;
    private final boolean fenced;

    public NodeImage(int nodeId, String rack, Map<String, Node> endpoints, boolean fenced) {
        this.nodeId = nodeId;
        this.rack = rack;
        this.endpoints = endpoints;
        this.fenced = fenced;
    }

    public int nodeId() {
        return nodeId;
    }

    public String rack() {
        return rack;
    }

    Map<String, Node> endpoints() {
        return endpoints;
    }

    public Node endpoint(String listenerName) {
        return endpoints.get(listenerName);
    }

    public boolean fenced() {
        return fenced;
    }
}
