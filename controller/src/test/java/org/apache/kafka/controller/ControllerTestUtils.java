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
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.EventQueue;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class ControllerTestUtils {
    /**
     * Create a new BrokerInfo instance for testing.
     *
     * @param id    The broker ID to use for the new BrokerInfo instance.
     * @return      The new BrokerInfo instance.
     */
    static BrokerInfo newBrokerInfo(int id) {
        EndPoint endPoint = new EndPoint("localhost", 9020,
            new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT);
        Broker broker = new Broker(id,
            JavaConverters.asScalaBuffer(Collections.singletonList(endPoint)).seq(),
            OptionConverters.<String>toScala(Optional.empty()));
        return new BrokerInfo(broker, 3, 8080);
    }

    /**
     * An event which will block the processing of further events on the event queue for
     * as long as desired.
     */
    static class BlockingEvent implements EventQueue.Event<Void> {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch completable = new CountDownLatch(1);

        @Override
        public Void run() {
            started.countDown();
            try {
                completable.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
}
