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

import kafka.utils.CoreUtils;
import kafka.zk.BrokerInfo;
import kafka.zk.KafkaZkClient;
import kafka.zk.ZkVersion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ZkBackingStoreTest {
    private static final Logger log = LoggerFactory.getLogger(ZkBackingStoreTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCreateAndClose() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (KafkaZkClient zkClient = zooKeeper.newKafkaZkClient()) {
                zkClient.createTopLevelPaths();
                try (ZkBackingStore store = ZkBackingStore.create(
                        new AtomicReference<>(null), 0, "", zkClient)) {
                    assertEquals(null, store.lastUnexpectedError());
                }
            }
        }
    }

    private static class TrackingActivationListener implements BackingStore.Activator,
            BackingStore.Controller {
        private boolean active;
        private final CountDownLatch hasActivated = new CountDownLatch(1);

        @Override
        synchronized public BackingStore.Controller activate(MetadataState newState,
                                                             int controllerEpoch) {
            this.active = true;
            hasActivated.countDown();
            return this;
        }

        @Override
        synchronized public void close() {
            this.active = false;
        }

        @Override
        synchronized public void handleBrokerUpdates(List<MetadataState.Broker> changedBrokers,
                                                     List<Integer> deletedBrokerIds) {
        }

        @Override
        synchronized public void handleTopicUpdates(List<MetadataState.Topic> changed,
                                                    List<String> deleted) {
        }

        synchronized boolean active() {
            return active;
        }
    }

    @Test
    public void testStartAndClose() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (KafkaZkClient zkClient = zooKeeper.newKafkaZkClient()) {
                zkClient.createTopLevelPaths();
                try (ZkBackingStore store = ZkBackingStore.create(
                        new AtomicReference<>(null), 0, "", zkClient)) {
                    BrokerInfo broker0Info = ControllerTestUtils.brokerToBrokerInfo(
                        ControllerTestUtils.newTestBroker(0));
                    TrackingActivationListener listener = new TrackingActivationListener();
                    CompletableFuture<Void> startFuture = store.start(broker0Info, listener);
                    startFuture.get();
                    listener.hasActivated.await();
                    store.shutdown();
                    assertEquals(null, store.lastUnexpectedError());
                }
            }
        }
    }

    private static class ZkBackingStoreEnsemble implements AutoCloseable {
        private final List<TrackingActivationListener> activationListeners;
        private final List<MetadataState.Broker> brokers;
        private final List<ZkBackingStore> stores;

        ZkBackingStoreEnsemble(CloseableEmbeddedZooKeeper zooKeeper,
                               int numStores) throws Exception {
            this.activationListeners = new ArrayList<>();
            for (int i = 0; i < numStores; i++) {
                this.activationListeners.add(new TrackingActivationListener());
            }
            this.brokers = new ArrayList<>(numStores);
            for (int i = 0; i < numStores; i++) {
                this.brokers.add(ControllerTestUtils.newTestBroker(i));
            }
            this.stores = new ArrayList<>(numStores);
            try {
                for (int i = 0; i < numStores; i++) {
                    KafkaZkClient zkClient = zooKeeper.newKafkaZkClient();
                    if (i == 0) {
                        zkClient.createTopLevelPaths();
                    }
                    stores.add(ZkBackingStore.create(new AtomicReference<>(null),
                        i, String.format("Node%d_", i), zkClient));
                }
            } catch (Exception e) {
                for (ZkBackingStore store : stores) {
                    store.close();
                    store.zkClient().close();
                }
                throw e;
            }
        }

        void updateBroker(MetadataState.Broker broker) {
            brokers.set(broker.brokerId(), broker);
            stores.get(broker.brokerId()).
                updateBrokerInfo(ControllerTestUtils.brokerToBrokerInfo(broker));
        }

        CompletableFuture<Void> start(int i) throws Exception {
            ZkBackingStore store = stores.get(i);
            return store.start(
                ControllerTestUtils.brokerToBrokerInfo(brokers.get(i)),
                activationListeners.get(i));
        }

        CompletableFuture<Void> startAll() throws Exception {
            List<CompletableFuture<Void>> startFutures = new ArrayList<>();
            for (int i = 0; i < stores.size(); i++) {
                ZkBackingStore store = stores.get(i);
                startFutures.add(store.start(
                    ControllerTestUtils.brokerToBrokerInfo(brokers.get(i)),
                    activationListeners.get(i)));
            }
            return ControllerTestUtils.allOf(startFutures);
        }

        int waitForSingleActive(int nodeIdToIgnore) throws Exception {
            final AtomicInteger activeNodeId = new AtomicInteger(-1);
            TestUtils.waitForCondition(() -> {
                int numActive = 0;
                activeNodeId.set(-1);
                for (int i = 0; i < stores.size(); i++) {
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

        void waitForActiveState(Consumer<MetadataState> callback)
                throws Exception {
            TestUtils.retryOnExceptionWithTimeout(() -> {
                int activeId = -1;
                for (int i = 0; i < stores.size(); i++) {
                    if (activationListeners.get(i).active) {
                        activeId = i;
                        break;
                    }
                }
                if (activeId == -1) {
                    throw new RuntimeException("there were no active stores");
                }
                MetadataState state = stores.get(activeId).metadataState();
                callback.accept(state);
            });
        }

        void waitForBrokers(List<MetadataState.Broker> expected) throws Exception {
            waitForActiveState(state -> {
                // Don't compare epochs.
                ControllerTestUtils.clearEpochs(state.brokers());
                if (!state.brokers().equalsIgnoringOrder(expected)) {
                    throw new RuntimeException("Expected brokers: " +
                        expected + ", actual brokers: " + state.brokers());
                }
            });
        }

        void waitForTopics(List<MetadataState.Topic> expected) throws Exception {
            waitForActiveState(state -> {
                if (!state.topics().equalsIgnoringOrder(expected)) {
                    throw new RuntimeException("Expected topics: " +
                        expected + ", actual topics: " + state.topics());
                }
            });
        }

        @Override
        public void close() throws InterruptedException {
            for (ZkBackingStore store : stores) {
                try {
                    store.shutdown();
                } catch (TimeoutException e) {
                    // ignore
                }
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
                     new ZkBackingStoreEnsemble(zooKeeper, 2)) {
                ensemble.startAll().get();
                int activeNodeId = ensemble.waitForSingleActive(-1);
                log.debug("Node {} is now the only active node.", activeNodeId);
                // Put a blocking event in the event queue of the ZkBackingStore which is
                // currently the leader, to ensure that it won't be elected a second time.
                ControllerTestUtils.BlockingEvent blockingEvent =
                    new ControllerTestUtils.BlockingEvent();
                int newActiveNodeId;
                try {
                    ensemble.stores.get(activeNodeId).backingStoreQueue().append(blockingEvent);
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
                ensemble.waitForBrokers(ensemble.brokers);
            }
        }
    }

    @Test
    public void testBrokerRegistration() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (ZkBackingStoreEnsemble ensemble =
                     new ZkBackingStoreEnsemble(zooKeeper, 3)) {

                // Test creating ZkBackingStores one by one and verifying that we tracked
                // the nodes getting registered.
                final int numBrokers = ensemble.brokers.size();
                for (int i = 0; i < numBrokers; i++) {
                    final int startedUpTo = i;
                    log.info("starting broker {}", i);
                    ensemble.start(i).get();
                    ensemble.waitForBrokers(ensemble.brokers.subList(0, startedUpTo + 1));
                }
                // Test changing broker information.
                ensemble.updateBroker(ensemble.brokers.get(numBrokers - 1).
                    duplicate().setRack("testRack"));
                ensemble.waitForBrokers(ensemble.brokers);

                // Test removing ZkBackingStores and verifying that the remaining ones
                // tracked the nodes going away.
                for (int i = 0; i < numBrokers - 1; i++) {
                    final int stoppedUpTo = i;
                    log.info("closing broker {}", i);
                    ensemble.stores.get(i).close();
                    ensemble.stores.get(i).zkClient().close();
                    ensemble.waitForBrokers(ensemble.brokers.subList(stoppedUpTo + 1, numBrokers));
                }
            }
        }
    }

    @Test
    public void testTopicCreation() throws Exception {
        try (CloseableEmbeddedZooKeeper zooKeeper = new CloseableEmbeddedZooKeeper()) {
            try (ZkBackingStoreEnsemble ensemble =
                     new ZkBackingStoreEnsemble(zooKeeper, 2)) {
                ensemble.startAll().get();
                ensemble.waitForTopics(Collections.emptyList());
                HashMap<TopicPartition, Seq<Object>> assignment = new HashMap<>();
                assignment.put(new TopicPartition("foo", 0),
                    CollectionConverters.asScala(Arrays.asList(0, 1, 2)));
                assignment.put(new TopicPartition("foo", 1),
                    CollectionConverters.asScala(Arrays.asList(1, 2, 3)));
                assignment.put(new TopicPartition("foo", 2),
                    CollectionConverters.asScala(Arrays.asList(2, 3, 0)));
                KafkaZkClient zkClient = ensemble.stores.get(0).zkClient();
                zkClient.createTopicAssignment("foo",
                    CoreUtils.toImmutableMap(CollectionConverters.asScala(assignment)));
                MetadataState.Topic foo = new MetadataState.Topic().setName("foo");
                foo.partitions().add(new MetadataState.Partition().setId(0).
                    setReplicas(Arrays.asList(0, 1, 2)));
                foo.partitions().add(new MetadataState.Partition().setId(1).
                    setReplicas(Arrays.asList(1, 2, 3)));
                foo.partitions().add(new MetadataState.Partition().setId(2).
                    setReplicas(Arrays.asList(2, 3, 0)));
                ensemble.waitForTopics(Collections.singletonList(foo));
                assignment.remove(new TopicPartition("foo", 2));
                foo.partitions().remove(new MetadataState.Partition().setId(2));
                zkClient.setTopicAssignment("foo",
                    CoreUtils.toImmutableMap(CollectionConverters.asScala(assignment)));
                ensemble.waitForTopics(Collections.singletonList(foo));
                zkClient.deleteTopicZNode("foo", -1);
                ensemble.waitForTopics(Collections.emptyList());
            }
        }
    }
}
