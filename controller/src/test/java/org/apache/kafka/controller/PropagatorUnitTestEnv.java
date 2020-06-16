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
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;

public class PropagatorUnitTestEnv implements AutoCloseable {
    private final ControllerLogContext logContext;
    private final MockTime time;
    private final ManualMetadataUpdater manualMetadataUpdater;
    private final MockClient.MockMetadataUpdater mockMetadataUpdater;
    private final MockClient client;
    private final KafkaConfig config;
    private final KafkaPropagator propagator;

    public PropagatorUnitTestEnv(String name) {
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
        this.config = ControllerTestUtils.newKafkaConfig(0);
        this.propagator = new KafkaPropagator(logContext, manualMetadataUpdater,
            client, time, config);
    }

    List<Node> nodes(int numNodes) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(new Node(i, "localhost", 9092 + i));
        }
        return nodes;
    }

    public ControllerLogContext logContext() {
        return logContext;
    }

    public MockTime time() {
        return time;
    }

    public ManualMetadataUpdater manualMetadataUpdater() {
        return manualMetadataUpdater;
    }

    public MockClient.MockMetadataUpdater mockMetadataUpdater() {
        return mockMetadataUpdater;
    }

    public MockClient client() {
        return client;
    }

    public KafkaConfig config() {
        return config;
    }

    public KafkaPropagator propagator() {
        return propagator;
    }

    @Override
    public void close() throws InterruptedException {
        propagator.close();
    }
}
