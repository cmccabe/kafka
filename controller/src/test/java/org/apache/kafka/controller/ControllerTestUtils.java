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
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.message.MetadataStateData;
import org.apache.kafka.common.message.MetadataStateData.BrokerEndpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

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
     * Create a new MetadataStateData.Broker instance for testing.
     *
     * @param id    The broker ID to use for the new MetadataStateData.Broker instance.
     * @return      The new MetadataStateData.Broker instance.
     */
    static MetadataStateData.Broker newTestBroker(int id) {
        MetadataStateData.Broker broker = new MetadataStateData.Broker();
        broker.setEndPoints(Collections.singletonList(
            new BrokerEndpoint().setHost("localhost").
                setPort((short) 9020).
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)));
        broker.setBrokerEpoch(-1);
        broker.setBrokerId(id);
        broker.setRack(null);
        return broker;
    }

    /**
     * Convert a MetadataStateData.Broker object into a kafka.zk.BrokerInfo class.
     *
     * @param stateBroker   The input object.
     * @return              The translated object.
     */
    public static BrokerInfo brokerToBrokerInfo(MetadataStateData.Broker stateBroker) {
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
        // We don't store the JMX port in MetadataStateData.Broker, so just make
        // something up.
        int jmxPort = 8686 + stateBroker.brokerId();
        // Use the latest version
        int version = 4;
        return new BrokerInfo(broker, version, jmxPort);
    }
}
