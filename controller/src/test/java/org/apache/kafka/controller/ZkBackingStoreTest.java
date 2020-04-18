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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.controller.BackingStore.ActivationListener;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;

public class ZkBackingStoreTest extends ZooKeeperTestHarness {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateAndClose() throws Exception {
        zkClient().createTopLevelPaths();
        try (ZkBackingStore store = ZkBackingStore.create(0, "", zkClient())) {
        }
    }

    @Test
    public void testStartAndClose() throws Exception {
        zkClient().createTopLevelPaths();
        try (ZkBackingStore store = ZkBackingStore.create(0, "", zkClient())) {
            BrokerInfo broker0Info = ControllerTestUtils.newBrokerInfo(0);
            final CountDownLatch hasActivated = new CountDownLatch(1);
            CompletableFuture<Void> startFuture =
                store.start(broker0Info, new ActivationListener() {
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
        }
    }

    private class ZkBackingStoreTestContext implements ActivationListener {
        private final ZkBackingStore store;
        private final int index;
        private final AtomicBoolean active = new AtomicBoolean(false);

        ZkBackingStoreTestContext(int index) {
            this.store = ZkBackingStore.create(index, "", zkClient());
            this.index = index;
        }

        CompletableFuture<Void> start() {
            return store.start(ControllerTestUtils.newBrokerInfo(index), this);
        }

        @Override
        public void activate(KafkaController newController) {
            active.set(true);
        }

        @Override
        public void deactivate() {
            active.set(false);
        }
    }

    @Test
    public void testOnlyOneActivates() throws Exception {
        zkClient().createTopLevelPaths();

        final int numStores = 5;
        List<ZkBackingStoreTestContext> storeContexts = new ArrayList<>(numStores);
        try {
            for (int i = 0; i < numStores; i++) {
                storeContexts.add(new ZkBackingStoreTestContext(i));
            }
            List<CompletableFuture<Void>> startFutures = new ArrayList<>();
            for (ZkBackingStoreTestContext context : storeContexts) {
                startFutures.add(context.start());
            }
            for (CompletableFuture<Void> startFuture : startFutures) {
                startFuture.get();
            }
            TestUtils.waitForCondition(() -> {
                int numActive = 0;
                for (ZkBackingStoreTestContext context : storeContexts) {
                    if (context.active.get()) {
                        numActive++;
                    }
                }
                return 1 == numActive;
            }, "waiting for a single controller to become active");
        } finally {
            for (ZkBackingStoreTestContext context : storeContexts) {
                context.store.shutdown(TimeUnit.SECONDS, 0);
            }
            for (ZkBackingStoreTestContext context : storeContexts) {
                context.store.close();
            }
        }
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
