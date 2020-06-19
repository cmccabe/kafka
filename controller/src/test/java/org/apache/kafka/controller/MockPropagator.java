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

import kafka.common.RequestAndCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockPropagator implements Propagator {
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private List<Node> nodes = Collections.emptyList();

    private final Map<Integer, List<RequestAndCompletionHandler>> inFlight = new HashMap<>();

    @Override
    public void send(List<Node> nodes, Collection<RequestAndCompletionHandler> requests) {
        this.nodes = nodes;
        for (RequestAndCompletionHandler request : requests) {
            List<RequestAndCompletionHandler> nodeRequests =
                inFlight.get(request.destination().id());
            if (nodeRequests == null) {
                nodeRequests = new ArrayList<>();
                inFlight.put(request.destination().id(), nodeRequests);
            }
            nodeRequests.add(request);
        }
    }

    public int numInFlight() {
        int numInFlight = 0;
        for (List<RequestAndCompletionHandler> requests : inFlight.values()) {
            numInFlight += requests.size();
        }
        return numInFlight;
    }

    public List<Node> nodes() {
        return nodes;
    }

    public RequestAndCompletionHandler getInFlightRequest(int node, ApiKeys apiKey) {
        List<RequestAndCompletionHandler> requests = inFlight.get(node);
        if (requests == null) {
            return null;
        }
        RequestAndCompletionHandler result = null;
        for (RequestAndCompletionHandler request : requests) {
            AbstractRequest abstractRequest = request.request().build();
            if (abstractRequest.api.equals(apiKey)) {
                if (result != null) {
                    throw new RuntimeException("More than one in-flight message with " +
                        "api key " + apiKey + " found for node " + node);
                }
                result = request;
            }
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
    }

    boolean closed() {
        return closed.get();
    }
}
