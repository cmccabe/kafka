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
import org.apache.kafka.common.utils.LogContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class KafkaControllerManagerTest {
    private static final Logger log =
        LoggerFactory.getLogger(KafkaControllerManagerTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCreateAndClose() throws Throwable {
        AtomicReference<Throwable> lastUnexpectedError = new AtomicReference<>();
        try (KafkaControllerManager manager =
                 new KafkaControllerManager("testCreateAndClose_",
                     new LogContext(String.format("[testCreateAndClose]")),
                     lastUnexpectedError,
                     new MockBackingStore.Builder().build())) {
        }
    }

    @Test
    public void testCreateStartAndClose() throws Throwable {
        AtomicReference<Throwable> lastUnexpectedError = new AtomicReference<>();
        try (KafkaControllerManager manager =
                 new KafkaControllerManager("testCreateStartAndClose_",
                     new LogContext(String.format("[testCreateStartAndClose]")),
                     lastUnexpectedError,
                     new MockBackingStore.Builder().build())) {
            BrokerInfo brokerInfo = ControllerTestUtils.brokerToBrokerInfo(
                ControllerTestUtils.newTestBroker(0));
            manager.start(brokerInfo).get();
            assertEquals("The ControllerManager has already been started.",
                assertThrows(ExecutionException.class, () -> manager.start(brokerInfo).get()).
                    getCause().getMessage());
        }
    }

//    @Test
//    public void testCreateAndStart() {
//
//    }
//
//    @Test
//    public void testCreateAndFailToStart() {
//
//    }

}
