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

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.message.MetadataState.BrokerEndpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.EventQueue;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ControllerTestUtils {
    private static final Logger log = LoggerFactory.getLogger(ControllerTestUtils.class);

    /**
     * An event which will block the processing of further events on the event queue for
     * as long as desired.
     */
    static class BlockingEvent implements EventQueue.Event<Void> {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch completable = new CountDownLatch(1);

        @Override
        public Void run() {
            log.info("Starting BlockingEvent.");
            started.countDown();
            try {
                completable.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                log.info("Finished BlockingEvent.");
            }
            return null;
        }

        public CountDownLatch started() {
            return started;
        }

        public CountDownLatch completable() {
            return completable;
        }
    }

    /**
     * Create a new MetadataState.Broker instance for testing.
     *
     * @param id    The broker ID to use for the new MetadataState.Broker instance.
     * @return      The new MetadataState.Broker instance.
     */
    static MetadataState.Broker newTestBroker(int id) {
        MetadataState.Broker broker = new MetadataState.Broker();
        broker.endPoints().getOrCreate("PLAINTEXT").
                setHost("localhost").
                setPort((short) 9020).
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);
        broker.setBrokerEpoch(-1);
        broker.setBrokerId(id);
        broker.setRack(null);
        return broker;
    }

    static void clearEpochs(MetadataState.BrokerCollection brokers) {
        for (MetadataState.Broker broker : brokers) {
            broker.setBrokerEpoch(-1);
        }
    }

    /**
     * Convert a MetadataState.Broker object into a kafka.zk.BrokerInfo object.
     *
     * @param stateBroker   The input object.
     * @return              The translated object.
     */
    public static BrokerInfo brokerToBrokerInfo(MetadataState.Broker stateBroker) {
        List<EndPoint> endpoints = new ArrayList<>();
        for (BrokerEndpoint endpoint : stateBroker.endPoints()) {
            SecurityProtocol securityProtocol =
                SecurityProtocol.forId(endpoint.securityProtocol());
            ListenerName listenerName = ListenerName.forSecurityProtocol(securityProtocol);
            endpoints.add(new EndPoint(endpoint.host(), endpoint.port(),
                listenerName, securityProtocol));
        }
        Optional<String> rack = Optional.ofNullable(stateBroker.rack());
        Broker broker = new Broker(stateBroker.brokerId(),
            CollectionConverters.asScala(endpoints),
            OptionConverters.toScala(rack));
        // We don't store the JMX port in MetadataState.Broker, so just make
        // something up.
        int jmxPort = 8686 + stateBroker.brokerId();
        // Use the latest version
        int version = 4;
        return new BrokerInfo(broker, version, jmxPort);
    }

    @SuppressWarnings("unchecked")
    public static CompletableFuture<Void> allOf(List<CompletableFuture<Void>> futures) {
        return CompletableFuture.allOf(futures.
            toArray(new CompletableFuture[futures.size()]));
    }

    public static void assertFutureExceptionEquals(Class<? extends Throwable> clazz,
                                                   CompletableFuture<?> future) {
        assertFutureExceptionEquals(clazz, null, future);
    }

    public static void assertFutureExceptionEquals(Class<? extends Throwable> clazz,
                                                   String messageSubString,
                                                   CompletableFuture<?> future) {
        ExecutionException executionException = Assert.assertThrows(
            ExecutionException.class, () -> future.get());
        assertEquals(clazz, executionException.getCause().getClass());
        if (messageSubString != null) {
            String messageText = executionException.getCause().getMessage();
            assertTrue("Expected exception message to include " + messageSubString +
                ", but it was " + messageText, messageText.contains(messageSubString));
        }
    }

    public static KafkaConfig newKafkaConfig(int brokerId, String... keysAndValues) {
        Properties props = new Properties();
        props.put(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
        props.put(KafkaConfig$.MODULE$.ZkConnectProp(), "localhost:2181");
        props.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        props.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), false);
        props.put(KafkaConfig$.MODULE$.BrokerIdProp(), Integer.toString(brokerId));
        if ((keysAndValues.length % 2) != 0) {
            throw new RuntimeException("Expected to see an even number of entries " +
                "arranged as key1, value1, key2, value2, ...");
        }
        for (int i = 0; i < keysAndValues.length; i+=2) {
            props.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return new KafkaConfig(props);
    }
}
