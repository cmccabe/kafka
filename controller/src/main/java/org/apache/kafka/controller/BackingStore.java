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

public interface BackingStore extends AutoCloseable {
    /**
     * Start this backing store.
     *
     * @param brokerInfo        The broker that we're registering.
     * @param callbackHandler   The callback handler to register.
     *
     * @return                  A future that is completed when we finish registering
     *                          with ZK.
     */
    CompletableFuture<Void> start(BrokerInfo brokerInfo,
                                  BackingStoreCallbackHandler callbackHandler);

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
     * Deactivate this BackingStore.
     *
     * @param controllerEpoch   The epoch of the controller to deactivate.
     *
     * @return                  A future that is completed once we know the deactivation
     *                          has been done.
     */
    CompletableFuture<Void> resign(int controllerEpoch);

    /**
     * Start the process of shutting down the backing store.
     */
    void beginShutdown();

    /**
     * Shut down the backing store and block until all resources and threads are
     * cleaned up.
     */
    void close() throws InterruptedException;
}
