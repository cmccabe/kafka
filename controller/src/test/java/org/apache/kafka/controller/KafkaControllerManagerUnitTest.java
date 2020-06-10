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
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaControllerManagerUnitTest {
    private static final Logger log =
        LoggerFactory.getLogger(KafkaControllerManagerUnitTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    private KafkaControllerManager createControllerManager(String name,
                                                           BackingStore backingStore,
                                                           Propagator propagator) {
        KafkaConfig config = ControllerTestUtils.newKafkaConfig(0);
        ControllerLogContext logContext = ControllerLogContext.fromPrefix(name);
        if (backingStore == null) {
            backingStore = new MockBackingStore.Builder().build();
        }
        if (propagator == null) {
            propagator = new MockPropagator();
        }
        KafkaEventQueue mainQueue =
            new KafkaEventQueue(logContext.logContext(), logContext.threadNamePrefix());
        return new KafkaControllerManager(logContext, config, backingStore, propagator,
            mainQueue);
    }

    @Test
    public void testCreateAndClose() throws Throwable {
        MockPropagator mockPropagator = new MockPropagator();
        assertFalse(mockPropagator.closed());
        try (KafkaControllerManager manager =
                 createControllerManager("testCreateAndClose", null, mockPropagator)) {
        }
        assertTrue(mockPropagator.closed());
    }

    @Test
    public void testCreateStartAndClose() throws Throwable {
        MockBackingStore backingStore = new MockBackingStore.Builder().build();
        try (KafkaControllerManager manager =
                 createControllerManager("testCreateStartAndClose", backingStore, null)) {
            BrokerInfo brokerInfo = ControllerTestUtils.brokerToBrokerInfo(
                ControllerTestUtils.newTestBroker(0));
            assertFalse(backingStore.isStarted());
            manager.start(brokerInfo).get();
            assertTrue(backingStore.isStarted());
            assertEquals("Attempting to Start a KafkaControllerManager which has " +
                "already been started.", assertThrows(ExecutionException.class,
                    () -> manager.start(brokerInfo).get()).getCause().getMessage());
            assertFalse(backingStore.isShutdown());
            manager.shutdown();
            TestUtils.waitForCondition(() -> backingStore.isShutdown(),
                "BackingStore shut down");
        }
    }

    @Test
    public void testStartError() throws Throwable {
        MockBackingStore backingStore = new MockBackingStore.Builder().
            setStartException(new RuntimeException("start error")).build();
        try (KafkaControllerManager manager =
                 createControllerManager("testStartError", backingStore, null)) {
            BrokerInfo brokerInfo = ControllerTestUtils.brokerToBrokerInfo(
                ControllerTestUtils.newTestBroker(0));
            assertFalse(backingStore.isStarted());
            ControllerTestUtils.assertFutureExceptionEquals(
                RuntimeException.class, manager.start(brokerInfo));
        }
    }
}
