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

import kafka.common.InterBrokerSendThread;
import kafka.common.RequestAndCompletionHandler;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

final public class KafkaPropagator implements Propagator {
    private final Logger log;

    /**
     * The cluster metadata manager used by the KafkaClient.
     */
    private final ManualMetadataUpdater metadataUpdater;

    /**
     * The network client used by this propagator.
     */
    private final KafkaClient client;

    /**
     * The time provider.
     */
    private final Time time;

    /**
     * The runnable used in the service thread for this propagator.
     */
    private final KafkaPropagatorSendThread thread;

    /**
     * The nodes information to use when sending messages.
     * Protected by the object monitor.
     */
    private List<Node> nodes = Collections.emptyList();

    /**
     * The requests to send.
     */
    private List<RequestAndCompletionHandler> requests = new ArrayList<>();

    class KafkaPropagatorSendThread extends InterBrokerSendThread {
        private final int requestTimeoutMs;

        public KafkaPropagatorSendThread(String name, KafkaClient client, Time time,
                                         int requestTimeoutMs) {
            super(name, client, time, true);
            this.requestTimeoutMs = requestTimeoutMs;
        }

        @Override
        public scala.collection.Iterable<RequestAndCompletionHandler> generateRequests() {
            Iterable<RequestAndCompletionHandler> iterable;
            List<Node> curNodes = null;
            synchronized (KafkaPropagator.this) {
                curNodes = nodes;
                metadataUpdater.setNodes(nodes);
                if (requests.isEmpty()) {
                    iterable = Collections.emptyList();
                } else {
                    iterable = requests;
                    requests = new ArrayList<>();
                }
            }
            if (log.isTraceEnabled()) {
                StringBuilder bld = new StringBuilder();
                int numRequests = 0;
                String prefix = "";
                for (Iterator<RequestAndCompletionHandler> i = iterable.iterator();
                         i.hasNext(); ) {
                    RequestAndCompletionHandler req = i.next();
                    bld.append(prefix).append("RequestAndCompletionHandler(");
                    bld.append("dest=").append(req.destination().id()).append(", ");
                    bld.append("builderClassName=").
                        append(req.request().getClass().getSimpleName());
                    bld.append(")");
                    numRequests++;
                    prefix = ", ";
                }
                log.trace("Generating {} request(s): [{}] to {} node(s) [{}]",
                    numRequests, bld.toString(), curNodes.size(), Utils.join(curNodes, ", "));
            }
            return CollectionConverters.asScala(iterable);
        }

        @Override
        public int requestTimeoutMs() {
            return requestTimeoutMs;
        }
    }

    @Override
    public void send(List<Node> nodes,
                     Collection<RequestAndCompletionHandler> newRequests) {
        synchronized (thread) {
            if (thread.isShutdownInitiated()) {
                cancelRequests(newRequests);
                return;
            }
            this.nodes = nodes;
            this.requests.addAll(newRequests);
            this.thread.wakeup();
        }
    }

    private void cancelRequests(Collection<RequestAndCompletionHandler> requests) {
        long nowMs = time.milliseconds();
        for (RequestAndCompletionHandler req : requests) {
            req.handler().onComplete(new ClientResponse(null, req.handler(),
                req.destination().idString(), nowMs, nowMs, true, null, null,
                null));
        }
    }

    public static KafkaPropagator create(ControllerLogContext logContext,
                                         KafkaConfig config,
                                         Metrics metrics) {
        ManualMetadataUpdater metadataUpdater = new ManualMetadataUpdater();
        String metricGrpPrefix = "propagator";
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        NetworkClient networkClient = null;
        Time time = SystemTime.SYSTEM;
        try {
            channelBuilder = ChannelBuilders.clientChannelBuilder(
                config.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                config,
                config.interBrokerListenerName(),
                config.saslMechanismInterBrokerProtocol(),
                time,
                config.saslInterBrokerHandshakeRequestEnable(),
                logContext.logContext());
            if (channelBuilder instanceof Reconfigurable) {
                config.addReconfigurable((Reconfigurable) channelBuilder);
            }
            selector = new Selector(config.connectionsMaxIdleMs(),
                metrics,
                time,
                metricGrpPrefix,
                channelBuilder,
                logContext.logContext());
            networkClient = new NetworkClient(
                selector,
                metadataUpdater,
                logContext.threadNamePrefix() + "Propagator",
                1,
                50,
                50,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.socketReceiveBufferBytes(),
                config.requestTimeoutMs(),
                ClientDnsLookup.DEFAULT,
                time,
                false,
                new ApiVersions(),
                logContext.logContext());
            return new KafkaPropagator(logContext, metadataUpdater, networkClient, time,
                config);
        } catch (Throwable e) {
            Utils.closeQuietly(channelBuilder, "KafkaPropagator#channelBuilder");
            Utils.closeQuietly(selector, "KafkaPropagator#selector");
            Utils.closeQuietly(networkClient, "KafkaPropagator#networkClient");
            throw new KafkaException("Failed to create new KafkaPropagator", e);
        }
    }

    KafkaPropagator(ControllerLogContext logContext,
                    ManualMetadataUpdater metadataUpdater,
                    KafkaClient client,
                    Time time,
                    KafkaConfig config) {
        this.log = logContext.createLogger(KafkaPropagator.class);
        this.metadataUpdater = metadataUpdater;
        this.client = client;
        this.time = time;
        this.thread = new KafkaPropagatorSendThread(
            logContext.threadNamePrefix() + "Propagator", client, time,
            config.requestTimeoutMs());
        this.thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        thread.shutdown();
        thread.join();
        Utils.closeQuietly(client, "KafkaPropagator#client");
        synchronized (thread) {
            cancelRequests(requests);
            nodes = null;
            requests = Collections.emptyList();
        }
    }
}
