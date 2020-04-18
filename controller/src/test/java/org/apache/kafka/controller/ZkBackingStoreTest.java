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
import kafka.zk.ZooKeeperTestHarness;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ZkBackingStoreTest extends ZooKeeperTestHarness {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateAndClose() throws Exception {
        zkClient().createTopLevelPaths();
        ZkBackingStore store = ZkBackingStore.create(0, "", zkClient());
        store.close();
    }

    @Test
    public void testStartAndClose() throws Exception {
        zkClient().createTopLevelPaths();
        ZkBackingStore store = ZkBackingStore.create(0, "", zkClient());
        BrokerInfo broker0Info = ControllerTestUtils.newBrokerInfo(0);
        final CountDownLatch hasActivated = new CountDownLatch(1);
        CompletableFuture<Void> startFuture =
            store.start(broker0Info, new BackingStore.ActivationListener() {
                @Override
                public void activate(KafkaController newController) {
                    hasActivated.countDown();
                }

                @Override
                public void deactivate() {
                }
            });
        startFuture.get();
        hasActivated.await();
        store.close();
    }

//    @Test
//    public void testStartAndClose() throws Exception {
//        zkClient().createTopLevelPaths();
//        ZkBackingStore store = ZkBackingStore.create(0, "", zkClient());
//        TestActivationListener testActivationListener = new TestActivationListener();
//        BrokerInfo broker0Info = ControllerTestUtils.newBrokerInfo(0);
//        CompletableFuture<Void> startFuture =
//            store.start(broker0Info, testActivationListener);
//        startFuture.get();
//        store.close();
//    }
}
