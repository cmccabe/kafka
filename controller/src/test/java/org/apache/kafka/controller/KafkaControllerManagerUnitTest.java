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

import kafka.zk.BrokerInfo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class KafkaControllerManagerUnitTest {
    private static final Logger log =
        LoggerFactory.getLogger(KafkaControllerManagerUnitTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCreateAndClose() throws Throwable {
        try (KafkaControllerManager manager = new KafkaControllerManager(
            ControllerLogContext.fromPrefix("testCreateAndClose"),
                     new MockBackingStore.Builder().build())) {
        }
    }

    @Test
    public void testCreateStartAndClose() throws Throwable {
        MockBackingStore backingStore = new MockBackingStore.Builder().build();
        try (KafkaControllerManager manager = new KafkaControllerManager(
                ControllerLogContext.fromPrefix("testCreateStartAndClose"), backingStore)) {
            BrokerInfo brokerInfo = ControllerTestUtils.brokerToBrokerInfo(
                ControllerTestUtils.newTestBroker(0));
            Assert.assertFalse(backingStore.isStarted());
            manager.start(brokerInfo).get();
            Assert.assertTrue(backingStore.isStarted());
            assertEquals("The ControllerManager has already been started.",
                assertThrows(ExecutionException.class, () -> manager.start(brokerInfo).get()).
                    getCause().getMessage());
            Assert.assertFalse(backingStore.isShutdown());
            manager.shutdown();
            Assert.assertTrue(backingStore.isShutdown());
        }
    }

    @Test
    public void testStartError() throws Throwable {
        MockBackingStore backingStore = new MockBackingStore.Builder().
            setStartException(new RuntimeException("start error")).build();
        try (KafkaControllerManager manager = new KafkaControllerManager(
                ControllerLogContext.fromPrefix("testStartError"), backingStore)) {
            BrokerInfo brokerInfo = ControllerTestUtils.brokerToBrokerInfo(
                ControllerTestUtils.newTestBroker(0));
            Assert.assertFalse(backingStore.isStarted());
            ControllerTestUtils.assertFutureExceptionEquals(
                RuntimeException.class, manager.start(brokerInfo));
            assertEquals(KafkaControllerManager.State.SHUT_DOWN, manager.state());
        }
    }
}
