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
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaPropagatorTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaPropagatorTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    static class MockPropagationManagerCallbackHandler
        implements PropagationManagerCallbackHandler {
        @Override
        public void handleLeaderAndIsrResponse(int controllerEpoch, int brokerId,
                                               ClientResponse response) {
        }

        @Override
        public void handleUpdateMetadataResponse(int controllerEpoch, int brokerId,
                                                 ClientResponse response) {
        }
    }

    static class TestEnv implements AutoCloseable {
        private final ControllerLogContext logContext;
        private final MockTime time;
        private final ManualMetadataUpdater manualMetadataUpdater;
        private final MockClient.MockMetadataUpdater mockMetadataUpdater;
        private final MockClient client;
        private final KafkaConfig config;
        private final KafkaPropagator propagator;
        private final PropagationManager propagationManager;
        private final MockPropagationManagerCallbackHandler callbackHandler;

        public TestEnv(String name) {
            this.logContext = ControllerLogContext.fromPrefix(name);
            this.time = new MockTime();
            this.manualMetadataUpdater = new ManualMetadataUpdater();
            this.mockMetadataUpdater = new MockClient.MockMetadataUpdater() {
                @Override
                public List<Node> fetchNodes() {
                    return manualMetadataUpdater.fetchNodes();
                }

                @Override
                public boolean isUpdateNeeded() {
                    return false;
                }

                @Override
                public void update(Time time, MockClient.MetadataUpdate update) {
                    throw new UnsupportedOperationException();
                }
            };
            this.client = new MockClient(time, mockMetadataUpdater);
            this.config = ControllerTestUtils.newKafkaConfig(0,
                KafkaConfig.ControlPlaneListenerNameProp(), "CONTROLLER",
                KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
                KafkaConfig.ListenersProp(), "CONTROLLER://localhost:9093,INTERNAL://localhost:9092",
                KafkaConfig.ListenerSecurityProtocolMapProp(), "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT");
            this.propagator = new KafkaPropagator(logContext, manualMetadataUpdater,
                client, time, config);
            this.callbackHandler = new MockPropagationManagerCallbackHandler();
            this.propagationManager = new PropagationManager(logContext, 0, 0,
                callbackHandler, config);
        }

        List<Node> nodes(int numNodes) {
            List<Node> nodes = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                nodes.add(new Node(i, "localhost", 9092 + i));
            }
            return nodes;
        }

        @Override
        public void close() throws InterruptedException {
            propagator.close();
        }
    }

    public class SimpleCreateTopicsRequestBuilder
            extends AbstractRequest.Builder<CreateTopicsRequest> {
        private final CreateTopicsRequestData data;

        public SimpleCreateTopicsRequestBuilder(CreateTopicsRequestData data, short version) {
            super(ApiKeys.forId(data.apiKey()), version);
            this.data = data;
        }

        @Override
        public CreateTopicsRequest build(short version) {
            return new CreateTopicsRequest(data, version);
        }
    }

    @Test
    public void testCreateAndClose() throws Throwable {
        try (TestEnv env = new TestEnv("testCreateAndClose")) {
        }
    }

    @Test
    public void testSendAndReceive() throws Throwable {
        short createTopicsVersion = 5;
        try (TestEnv env = new TestEnv("testSendAndReceive")) {
            List<Node> nodes = env.nodes(4);
            CreateTopicsRequestData reqData = new CreateTopicsRequestData().
                setTimeoutMs(12345).setValidateOnly(true);
            CreateTopicsResponseData respData = new CreateTopicsResponseData().
                setThrottleTimeMs(6789);
            CountDownLatch responseReceived = new CountDownLatch(1);
            env.client.prepareResponseFrom(new CreateTopicsResponse(respData), nodes.get(1));
            env.propagator.send(nodes, Collections.singletonList(
                new RequestAndCompletionHandler(nodes.get(1),
                    new SimpleCreateTopicsRequestBuilder(reqData, createTopicsVersion),
                    new RequestCompletionHandler() {
                        @Override
                        public void onComplete(ClientResponse response) {
                            assertEquals(null, response.authenticationException());
                            assertEquals(nodes.get(1).idString(), response.destination());
                            assertTrue(response.hasResponse());
                            CreateTopicsResponse resp =
                                (CreateTopicsResponse) response.responseBody();
                            assertEquals(respData, resp.data());
                            responseReceived.countDown();
                        }
                    })));
            env.time.sleep(1);
            responseReceived.await();
        }
    }

    @Test
    public void testCancelRequestsOnClose() throws Throwable {
        short createTopicsVersion = 5;
        CreateTopicsRequestData reqData = new CreateTopicsRequestData().
            setTimeoutMs(12345).setValidateOnly(true);
        List<Node> nodes = null;
        TestEnv env = new TestEnv("testCancelRequestsOnClose");
        try {
            nodes = env.nodes(4);
        } finally {
            env.close();
        }
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        env.propagator.send(nodes, Collections.singletonList(
            new RequestAndCompletionHandler(nodes.get(1),
                new SimpleCreateTopicsRequestBuilder(reqData, createTopicsVersion),
                response -> future.complete(response))));
        assertEquals("1", future.get().destination());
        assertEquals(null, future.get().versionMismatch());
        assertEquals(null, future.get().authenticationException());
        assertTrue(future.get().wasDisconnected());
    }
}
