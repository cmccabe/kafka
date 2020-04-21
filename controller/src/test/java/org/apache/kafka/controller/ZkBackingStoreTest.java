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
import kafka.zk.KafkaZkClient;
import kafka.zk.ZkVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.MetadataStateData;
import org.apache.kafka.controller.BackingStore.ActivationListener;
import org.apache.kafka.controller.ControllerTestUtils.BlockingEvent;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ZkBackingStoreTest {
    private static final Logger log = LoggerFactory.getLogger(ZkBackingStoreTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(60);

    @Test
    public void testCreateAndClose() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (KafkaZkClient zkClient = zooKeeper.newKafkaZkClient()) {
                zkClient.createTopLevelPaths();
                try (ZkBackingStore store = ZkBackingStore.create(0, "", zkClient)) {
                }
            }
        }
    }

    @Test
    public void testStartAndClose() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (KafkaZkClient zkClient = zooKeeper.newKafkaZkClient()) {
                zkClient.createTopLevelPaths();
                try (ZkBackingStore store = ZkBackingStore.create(0, "", zkClient)) {
                    BrokerInfo broker0Info = ControllerTestUtils.brokerToBrokerInfo(
                        ControllerTestUtils.newTestBroker(0));
                    final CountDownLatch hasActivated = new CountDownLatch(1);
                    CompletableFuture<Void> startFuture =
                        store.start(broker0Info, new ActivationListener() {
                            @Override
                            public void activate(MetadataStateData newState) {
                                hasActivated.countDown();
                            }

                            @Override
                            public void deactivate() {
                            }
                        });
                    startFuture.get();
                    hasActivated.await();
                    store.shutdown(TimeUnit.NANOSECONDS, 100);
                }
            }
        }
    }

    private static class TrackingActivationListener implements ActivationListener {
        private boolean active;

        @Override
        synchronized public void activate(MetadataStateData newState) {
            this.active = true;
        }

        @Override
        synchronized public void deactivate() {
            this.active = false;
        }

        synchronized boolean active() {
            return active;
        }
    }

    private static class ZkBackingStoreEnsemble implements AutoCloseable {
        private final List<TrackingActivationListener> activationListeners;
        private final List<MetadataStateData.Broker> brokers;
        private final List<ZkBackingStore> stores;

        ZkBackingStoreEnsemble(CloseableEmbeddedZooKeeper zooKeeper,
                               int numStores, int numBrokers) throws Exception {
            this.activationListeners = new ArrayList<>();
            for (int i = 0; i < numStores; i ++) {
                this.activationListeners.add(new TrackingActivationListener());
            }
            this.brokers = new ArrayList<>(numBrokers);
            for (int i = 0; i < numBrokers; i++) {
                this.brokers.add(ControllerTestUtils.newTestBroker(i));
            }
            this.stores = new ArrayList<>(numStores);
            try {
                for (int i = 0; i < numStores; i++) {
                    KafkaZkClient zkClient = zooKeeper.newKafkaZkClient();
                    if (i == 0) {
                        zkClient.createTopLevelPaths();
                    }
                    stores.add(ZkBackingStore.create(i, String.format("Node%d_", i),
                        zkClient));
                }
            } catch (Exception e) {
                for (ZkBackingStore store : stores) {
                    store.close();
                    store.zkClient().close();
                }
                throw e;
            }
        }

        void startAll() throws Exception {
            List<CompletableFuture<Void>> startFutures = new ArrayList<>();
            for (int i = 0; i < stores.size(); i++) {
                ZkBackingStore store = stores.get(i);
                startFutures.add(store.start(
                    ControllerTestUtils.brokerToBrokerInfo(brokers.get(i)),
                    activationListeners.get(i)));
            }
            for (CompletableFuture<Void> startFuture : startFutures) {
                startFuture.get();
            }
        }

        int waitForSingleActive(int nodeIdToIgnore) throws Exception {
            final AtomicInteger activeNodeId = new AtomicInteger(-1);
            TestUtils.waitForCondition(() -> {
                int numActive = 0;
                activeNodeId.set(-1);
                for (int i = 0 ; i < stores.size(); i++) {
                    if (activationListeners.get(i).active) {
                        if (i != nodeIdToIgnore) {
                            numActive++;
                            activeNodeId.set(i);
                        }
                    }
                }
                return 1 == numActive && activeNodeId.get() != -1;
            }, "waiting for a single controller to become active");
            return activeNodeId.get();
        }

        @Override
        public void close() throws InterruptedException {
            for (ZkBackingStore store : stores) {
                store.shutdown(TimeUnit.SECONDS, 0);
            }
            for (ZkBackingStore store : stores) {
                store.close();
                store.zkClient().close();
            }
        }
    }

    @Test
    public void testOnlyOneActivates() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (ZkBackingStoreEnsemble ensemble =
                     new ZkBackingStoreEnsemble(zooKeeper, 2, 2)) {
                ensemble.startAll();
                int activeNodeId = ensemble.waitForSingleActive(-1);
                log.debug("Node {} is now the only active node.", activeNodeId);
                // Put a blocking event in the event queue of the ZkBackingStore which is
                // currently the leader, to ensure that it won't be elected a second time.
                BlockingEvent blockingEvent = new BlockingEvent();
                int newActiveNodeId;
                try {
                    ensemble.stores.get(activeNodeId).eventQueue().append(blockingEvent);
                    blockingEvent.started().await();
                    // Trigger controller election.
                    ensemble.stores.get(0).zkClient().
                        deleteController(ZkVersion.MatchAnyVersion());
                    // Wait for a new controller to be elected.
                    newActiveNodeId = ensemble.waitForSingleActive(activeNodeId);
                    log.debug("Node {} is the new active node.", newActiveNodeId);
                    assertFalse("Controller ID " + activeNodeId + " was active both " +
                        "before and after the election.", activeNodeId == newActiveNodeId);
                } finally {
                    blockingEvent.completable().countDown();
                }
                assertEquals(newActiveNodeId, ensemble.waitForSingleActive(-1));
            }
        }
    }
}
