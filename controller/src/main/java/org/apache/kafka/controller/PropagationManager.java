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

import kafka.server.KafkaConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropagationManager {
    private final ControllerLogContext logContext;

    /**
     * The logger to use.
     */
    private final Logger log;

    /**
     * The listener name to use to when contacting other brokers.
     * Since brokers can have multiple listeners, corresponding to multiple open ports,
     * we need to know which ones to connect to.
     */
    private final String listenerName;

    /**
     * Maps brokers to outstanding messages to send.
     */
    private final Map<Integer, DestinationBroker> brokers;

    static String calculateListenerName(KafkaConfig config) {
        ListenerName name = OptionConverters.toJava(config.controlPlaneListenerName()).
            orElseGet(() -> config.interBrokerListenerName());
        return name.value();
    }

    Map<Integer, DestinationBroker> initializeBrokers(MetadataState state) {
        Map<Integer, DestinationBroker> brokers = new HashMap<>(state.brokers().size());
        for (MetadataState.Broker broker : state.brokers()) {
            try {
                MetadataState.BrokerEndpoint endpoint = broker.endPoints().find(listenerName);
                if (endpoint == null) {
                    throw new RuntimeException("No such listener as " + listenerName);
                } else if (SecurityProtocol.forId(endpoint.securityProtocol()) == null) {
                    throw new RuntimeException("Unknown security protocol " +
                        endpoint.securityProtocol());
                }
                brokers.put(broker.brokerId(), new DestinationBroker(broker.brokerId(),
                        endpoint.duplicate(), broker.rack()));
            } catch (Throwable e) {
                logContext.setLastUnexpectedError(log, "Failed to find controller " +
                    "listener for broker " + broker.brokerId(), e);
            }
        }
        return brokers;
    }

    static class DestinationBroker {
        private final int id;
        private MetadataState.BrokerEndpoint endpoint;
        private boolean endpointInfoDirty;
        private String rack;

        DestinationBroker(int id, MetadataState.BrokerEndpoint endpoint, String rack) {
            this.id = id;
            this.endpoint = endpoint;
            this.endpointInfoDirty = true;
            this.rack = rack;
        }
    }

    PropagationManager(ControllerLogContext logContext, KafkaConfig config,
                       MetadataState state) {
        this.logContext = logContext;
        this.log = logContext.createLogger(PropagationManager.class);
        this.listenerName = calculateListenerName(config);
        this.brokers = initializeBrokers(state);
    }

    public void handleBrokerUpdates(ReplicationManager replicationManager,
                                    BrokerDelta delta) {
    }

    public void handleTopicUpdates(ReplicationManager replicationManager,
                                   TopicDelta delta) {
    }

    public void propagate(Propagator propagator) {
        doPropagate(propagator, true);
    }

    public void maybePropagate(Propagator propagator) {
        doPropagate(propagator, false);
    }

    private void doPropagate(Propagator propagator, boolean force) {
        maybeUpdateBrokerEndpoints(propagator);
    }

    private void maybeUpdateBrokerEndpoints(Propagator propagator) {
        List<Node> nodes = new ArrayList<>();
        for (DestinationBroker broker : brokers.values()) {
            if (broker.endpointInfoDirty) {
                nodes.add(new Node(broker.id, broker.endpoint.host(),
                    broker.endpoint.port(), broker.rack));
            }
            broker.endpointInfoDirty = false;
        }
        if (!nodes.isEmpty()) {
            //propagator.setBrokerEndpoints(nodes);
        }
    }

    public void close(Propagator propagator) {
    }
}
