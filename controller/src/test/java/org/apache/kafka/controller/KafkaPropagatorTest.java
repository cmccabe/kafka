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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaPropagatorTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaPropagatorTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

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
        try (PropagatorUnitTestEnv env =
                 new PropagatorUnitTestEnv("testCreateAndClose")) {
        }
    }

    @Test
    public void testCreateStartAndClose() throws Throwable {
        try (PropagatorUnitTestEnv env =
                 new PropagatorUnitTestEnv("testCreateAndClose")) {
            env.propagator().start();
        }
    }

    @Test
    public void testSendAndReceive() throws Throwable {
        short createTopicsVersion = 5;
        try (PropagatorUnitTestEnv env =
                 new PropagatorUnitTestEnv("testSendAndReceive")) {
            env.propagator().start();
            List<Node> nodes = env.nodes(4);
            CreateTopicsRequestData reqData = new CreateTopicsRequestData().
                setTimeoutMs(12345).setValidateOnly(true);
            CreateTopicsResponseData respData = new CreateTopicsResponseData().
                setThrottleTimeMs(6789);
            CountDownLatch responseReceived = new CountDownLatch(1);
            env.client().prepareResponseFrom(new CreateTopicsResponse(respData), nodes.get(1));
            env.propagator().send(nodes, Collections.singletonList(
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
            env.time().sleep(1);
            responseReceived.await();
        }
    }
}
