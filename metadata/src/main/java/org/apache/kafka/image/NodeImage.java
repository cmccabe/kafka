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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents a single node in the ClusterImage.
 *
 * This class is thread-safe.
 */
public final class NodeImage {
    private final int nodeId;
    private final String rack;
    private final long epoch;
    private final Uuid incarnationId;
    private final Map<String, Node> endpoints;
    private final Map<String, SecurityProtocol> endpointSecurity;
    private final boolean fenced;
    //private Map<String, VersionRange> supportedFeatures();

    public NodeImage(RegisterBrokerRecord record) {
        this.nodeId = record.brokerId();
        this.rack = record.rack();
        this.epoch = record.brokerEpoch();
        this.incarnationId = record.incarnationId();
        this.endpoints = new HashMap<>();
        this.endpointSecurity = new HashMap<>();
        for (BrokerEndpoint endPoint : record.endPoints()) {
            this.endpoints.put(endPoint.name(),
                new Node(nodeId, endPoint.host(), endPoint.port(), record.rack()));
            this.endpointSecurity.put(endPoint.name(),
                SecurityProtocol.forId(endPoint.securityProtocol()));
        }
        this.fenced = true;
    }

    public NodeImage(int nodeId,
                     String rack,
                     long epoch,
                     Uuid incarnationId,
                     Map<String, Node> endpoints,
                     Map<String, SecurityProtocol> endpointSecurity,
                     boolean fenced) {
        this.nodeId = nodeId;
        this.rack = rack;
        this.epoch = epoch;
        this.incarnationId = incarnationId;
        this.endpoints = endpoints;
        this.endpointSecurity = endpointSecurity;
        this.fenced = fenced;
    }

    public NodeImage(NodeImage prev, boolean fenced) {
        this.nodeId = prev.nodeId();
        this.rack = prev.rack();
        this.epoch = prev.epoch;
        this.incarnationId = prev.incarnationId;
        this.endpoints = prev.endpoints();
        this.endpointSecurity = prev.endpointSecurity;
        this.fenced = fenced;
    }

    public int nodeId() {
        return nodeId;
    }

    public String rack() {
        return rack;
    }

    public long epoch() {
        return epoch;
    }

    public Uuid incarnationId() {
        return incarnationId;
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

//    public List<ApiMessageAndVersion> toRecords() {
//        List<ApiMessageAndVersion> records = new ArrayList<>();
//        RegisterBrokerRecord registrationRecord = new RegisterBrokerRecord().
//            setBrokerId(nodeId).
//            setRack(rack).
//            setBrokerEpoch(epoch).
//            setIncarnationId(incarnationId);
//        for (Entry<String, Node> entry : endpoints.entrySet()) {
//            Node node = entry.getValue();
//            registrationRecord.endPoints().add(new BrokerEndpoint().
//                setName(entry.getKey()).
//                setHost(node.host()).
//                setPort(node.port()).
//                setSecurityProtocol(endpointSecurity.get(entry.getKey()).id));
//        }
//        registrationRecord.features()
//    }
}
