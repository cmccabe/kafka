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

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import scala.collection.Seq;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.Assert.assertEquals;

public class ControllerUtilsTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testExceptionalFuture() throws Exception {
        RuntimeException exception = new RuntimeException("foo");
        try {
            ControllerUtils.exceptionalFuture(exception).get();
            Assert.fail("expected an execution exception.");
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testBrokerToBrokerState() {
        Seq<EndPoint> endPoints = CollectionConverters.asScala(Arrays.asList(
            new EndPoint("host1", 123, new ListenerName("B"), SecurityProtocol.SSL),
            new EndPoint("host2", 123, new ListenerName("A"), SecurityProtocol.PLAINTEXT)));
        Broker broker = new Broker(3, endPoints, OptionConverters.toScala(Optional.empty()));
        MetadataState.Broker brokerState = ControllerUtils.brokerToBrokerState(broker);
        assertEquals(3, brokerState.brokerId());
        MetadataState.BrokerEndpointCollection endpoints =
            new MetadataState.BrokerEndpointCollection();
        endpoints.getOrCreate("A").setHost("host2").setPort((short) 123).
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);
        endpoints.getOrCreate("B").setHost("host1").setPort((short) 123).
            setSecurityProtocol(SecurityProtocol.SSL.id);
        assertEquals(endpoints, brokerState.endPoints());
        assertEquals(null, brokerState.rack());
    }

    @Test
    public void testNanosToFractionalMillis() throws Exception {
        assertEquals("123", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(123)));
        assertEquals("0", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(0)));
        assertEquals("1", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(1)));
        assertEquals("1.2346", ControllerUtils.
            nanosToFractionalMillis(1234567));
    }
}
