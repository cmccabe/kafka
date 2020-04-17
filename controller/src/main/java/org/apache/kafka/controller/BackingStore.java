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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface BackingStore extends AutoCloseable {
    /**
     * Listens for controller activation and deactivation events.
     */
    interface ActivationListener {
        void activate(KafkaController newController);
        void deactivate();
    }

    /**
     * Start this backing store.
     *
     * @param brokerInfo    A listener for controller activation and deactivation events.
     * @param listener      A listener for controller activation and deactivation events.
     *
     * @return              A future that is completed when we finish registering with ZK.
     */
    CompletableFuture<Void> start(BrokerInfo brokerInfo, ActivationListener listener);

    /**
     * Change the broker information.
     *
     * @param newBrokerInfo     The new broker information.
     *
     * @return                  A future that is completed when we finish storing the new
     *                          broker information.
     */
    CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo);

    /**
     * Shut down the backing store after the given amount of time.
     *
     * @param timeUnit      The time unit to use for the timeout.
     * @param timeSpan      The amount of time to use for the timeout.
     *                      Once the timeout elapses, any remaining queued
     *                      events will get a
     *                      @{org.apache.kafka.common.errors.TimeoutException},
     *                      as will any subsequent operations.
     */
    void shutdown(TimeUnit timeUnit, long timeSpan);

    /**
     * Synchronously close the backing store and wait for any threads to be joined.
     */
    void close() throws InterruptedException;
}
