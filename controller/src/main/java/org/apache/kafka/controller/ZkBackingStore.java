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
import kafka.controller.ReplicaAssignment;
import kafka.zk.BrokerIdZNode;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.BrokerInfo;
import kafka.zk.ControllerZNode;
import kafka.zk.KafkaZkClient;
import kafka.zk.KafkaZkClient.BrokerAndEpoch;
import kafka.zk.StateChangeHandlers;
import kafka.zk.TopicZNode;
import kafka.zk.TopicsZNode;
import kafka.zookeeper.StateChangeHandler;
import kafka.zookeeper.ZNodeChangeHandler;
import kafka.zookeeper.ZNodeChildChangeHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ZkBackingStore implements BackingStore {
    private final AtomicReference<Throwable> lastUnexpectedError;
    private final Logger log;
    private final int nodeId;
    private final EventQueue backingStoreQueue;
    private final KafkaZkClient zkClient;
    private final ZkStateChangeHandler zkStateChangeHandler;
    private final ControllerChangeHandler controllerChangeHandler;
    private final BrokerChildChangeHandler brokerChildChangeHandler;
    private final Map<Integer, BrokerChangeHandler> brokerChangeHandlers;
    private final TopicChildChangeHandler topicChildChangeHandler;
    private final Map<String, TopicChangeHandler> topicChangeHandlers;
    private boolean started;
    private Activator activator;
    private long brokerEpoch;
    private BrokerInfo brokerInfo;
    private int activeId;
    private ActiveZkBackingStoreState activeState;

    static class ActiveZkBackingStoreState {
        final int controllerEpoch;
        final int epochZkVersion;
        final MetadataState metadata;
        final Controller controller;

        ActiveZkBackingStoreState(int controllerEpoch,
                                  int epochZkVersion,
                                  MetadataState metadata,
                                  Controller controller) {
            this.controllerEpoch = controllerEpoch;
            this.epochZkVersion = epochZkVersion;
            this.metadata = metadata;
            this.controller = controller;
        }
    }

    /**
     * Handles the ZooKeeper session getting dropped or re-established.
     */
    class ZkStateChangeHandler implements StateChangeHandler {
        @Override
        public String name() {
            return StateChangeHandlers.ControllerHandler();
        }

        @Override
        public void beforeInitializingSession() {
            // Handle the ZooKeeper session dropping.  We immediately cancel all events
            // that would require us to be the controller.  We can't be the controller
            // without a ZK session.  Then we prepend the SessionExpirationEvent to the
            // queue, so that it is handled first.  It will trigger our resignation, if
            // needed.
            CompletableFuture<Void> future = backingStoreQueue.enqueue(false,
                ZkSessionDropCanceller.INSTANCE,
                null,
                new SessionExpirationEvent());
            // Block until the SessionExpirationEvent is handled.  This ensures that we
            // have resigned and cleared the controller state before we try to
            // re-establish the connection to ZK.
            ControllerUtils.await(log, future);
        }

        @Override
        public void afterInitializingSession() {
            // Once we have a new ZK session, try to become the active controller.
            backingStoreQueue.append(new TryToActivateEvent(true));
        }
    }

    static class ZkSessionDropCanceller implements Function<EventQueue.Event<?>, Throwable> {
        final static ZkSessionDropCanceller INSTANCE = new ZkSessionDropCanceller();

        @Override
        public Throwable apply(EventQueue.Event<?> event) {
            if (!(event instanceof AbstractZkBackingStoreEvent)) {
                return null;
            }
            AbstractZkBackingStoreEvent<?> e = (AbstractZkBackingStoreEvent<?>) event;
            if (e.cancelIfZkSessionDrops) {
                return new NotControllerException("Zookeeper session expired");
            } else {
                return null;
            }
        }
    }

    /**
     * Handles the /controller znode changing.
     */
    class ControllerChangeHandler implements ZNodeChangeHandler {
        @Override
        public String path() {
            return ControllerZNode.path();
        }

        @Override
        public void handleCreation() {
            backingStoreQueue.append(new ControllerChangeEvent());
        }

        @Override
        public void handleDeletion() {
            backingStoreQueue.append(new TryToActivateEvent(false));
        }

        @Override
        public void handleDataChange() {
            backingStoreQueue.append(new ControllerChangeEvent());
        }
    }

    /**
     * Handles changes to the children of the /broker path.
     */
    class BrokerChildChangeHandler implements ZNodeChildChangeHandler {
        @Override
        public String path() {
            return BrokerIdsZNode.path();
        }

        @Override
        public void handleChildChange() {
            backingStoreQueue.append(new BrokerChildChangeEvent());
        }
    }

    /**
     * Handles changes to a specific broker ZNode /broker/$ID
     */
    class BrokerChangeHandler implements ZNodeChangeHandler {
        private final int brokerId;

        BrokerChangeHandler(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public String path() {
            return BrokerIdZNode.path(brokerId);
        }

        @Override
        public void handleDataChange() {
            backingStoreQueue.append(new BrokerChangeEvent(brokerId));
        }
    }

    /**
     * Handles changes to the children of the /broker/topics path.
     */
    class TopicChildChangeHandler implements ZNodeChildChangeHandler {
        @Override
        public String path() {
            return TopicsZNode.path();
        }

        @Override
        public void handleChildChange() {
            backingStoreQueue.append(new TopicChildChangeEvent());
        }
    }

    /**
     * Handles changes to a /broker/topics/$topic path.
     */
    class TopicChangeHandler implements ZNodeChangeHandler {
        private final String topic;

        TopicChangeHandler(String topic) {
            this.topic = topic;
        }

        @Override
        public String path() {
            return TopicZNode.path(topic);
        }

        @Override
        public void handleDataChange() {
            backingStoreQueue.append(new TopicChangeEvent(topic));
        }
    }

    private void registerBrokerChangeHandler(int brokerId) {
        BrokerChangeHandler changeHandler = new BrokerChangeHandler(brokerId);
        zkClient.registerZNodeChangeHandlerAndCheckExistence(changeHandler);
        brokerChangeHandlers.put(brokerId, changeHandler);
    }

    private void unregisterBrokerChangeHandler(int brokerId) {
        BrokerChangeHandler handler = brokerChangeHandlers.remove(brokerId);
        if (handler != null) {
            zkClient.unregisterZNodeChangeHandler(handler.path());
        }
    }

    private void registerTopicChangeHandler(String name) {
        TopicChangeHandler changeHandler = new TopicChangeHandler(name);
        zkClient.registerZNodeChangeHandlerAndCheckExistence(changeHandler);
        topicChangeHandlers.put(name, changeHandler);
    }

    private void unregisterTopicChangeHandler(String name) {
        TopicChangeHandler handler = topicChangeHandlers.remove(name);
        if (handler != null) {
            zkClient.unregisterZNodeChangeHandler(handler.path());
        }
    }

    abstract class AbstractZkBackingStoreEvent<T> extends AbstractEvent<T> {
        private final boolean cancelIfZkSessionDrops;

        public AbstractZkBackingStoreEvent(boolean cancelIfZkSessionDrops) {
            super(log);
            this.cancelIfZkSessionDrops = cancelIfZkSessionDrops;
        }

        @Override
        public Throwable handleException(Throwable e) throws Throwable {
            if (e instanceof ControllerMovedException) {
                resignIfActive(true, "The controller got a ControllerMovedException.");
                return new NotControllerException(e.getMessage());
            } else {
                lastUnexpectedError.set(e);
                resignIfActive(true, "The controller had an unexpected error.");
                return e;
            }
        }
    }

    /**
     * Check that the ZkBackingStore has been started.  This is a sanity check that
     * the developer remembered to call start().
     */
    private void checkIsStarted() {
        if (!started) {
            throw new RuntimeException("The ZkBackingStore has not been started.");
        }
    }

    /**
     * Check that the ZkBackingStore has been started and is currently active.
     * Of course, there is a race condition here: we may think that we are still
     * active when actually another node has already been elected.  To prevent problems
     * resulting from this, we use the controllerEpochZkVersion to fence certain updates
     * we make to ZooKeeper.
     *
     * So think of this check as a way to avoid unecessary load on ZooKeeper, but not as
     * something essential to correctness.
     */
    private void checkIsStartedAndActive() {
        checkIsStarted();
        if (activeState == null) {
            throw new ControllerMovedException("This node is not the " +
                "active controller.");
        }
    }

    /**
     * Resign if we are the active controller.
     *
     * @param deleteZnode   True if we should delete the controller znode.
     *                      We only need to do this if we need to forcce a new election.
     * @param logSuffix     What to include in the log message.
     */
    private void resignIfActive(boolean deleteZnode, String logSuffix) {
        if (activeState == null) {
            log.info("{}", logSuffix);
            return;
        }
        log.warn("Resigning because {}{}",
            logSuffix.substring(0, 1).toLowerCase(Locale.ROOT),
            logSuffix.substring(1));
        try {
            zkClient.unregisterZNodeChildChangeHandler(brokerChildChangeHandler.path());
            zkClient.unregisterZNodeChildChangeHandler(topicChildChangeHandler.path());
            for (Iterator<BrokerChangeHandler> iter =
                 brokerChangeHandlers.values().iterator(); iter.hasNext(); ) {
                zkClient.unregisterZNodeChangeHandler(iter.next().path());
                iter.remove();
            }
            for (Iterator<TopicChangeHandler> iter =
                 topicChangeHandlers.values().iterator(); iter.hasNext(); ) {
                zkClient.unregisterZNodeChangeHandler(iter.next().path());
                iter.remove();
            }
            activeState.controller.close();
        } catch (Throwable e) {
            log.error("Unable to unregister ZkClient watches.", e);
            lastUnexpectedError.set(e);
        }
        if (deleteZnode) {
            try {
                zkClient.deleteController(activeState.epochZkVersion);
            } catch (ControllerMovedException e) {
                log.info("Tried to delete {} during resignation, but it has already " +
                    "been modified.", ControllerZNode.path());
            } catch (Throwable e) {
                log.error("Unable to delete {} during resignation due to unexpected " +
                    "error.", e);
                lastUnexpectedError.set(e);
            }
        }
        activeId = -1;
        activeState = null;
    }

    /**
     * Start the ZkBackingStore.  This should be done before any other operation.
     */
    class StartEvent extends AbstractZkBackingStoreEvent<Void> {
        private final BrokerInfo newBrokerInfo;
        private final Activator newActivator;

        StartEvent(BrokerInfo newBrokerInfo, Activator newActivator) {
            super(false);
            this.newBrokerInfo = newBrokerInfo;
            this.newActivator = newActivator;
        }

        @Override
        public Void execute() {
            if (started) {
                throw new RuntimeException("Attempting to Start a BackingStore " +
                    "which has already been started.");
            }
            zkClient.registerStateChangeHandler(zkStateChangeHandler);
            brokerEpoch = zkClient.registerBroker(newBrokerInfo);
            zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
            brokerInfo = newBrokerInfo;
            backingStoreQueue.append(new TryToActivateEvent(false));
            activator = newActivator;
            started = true;
            log.info("{}: initialized ZkBackingStore with brokerInfo {}.",
                name, newBrokerInfo);
            return null;
        }
    }

    /**
     * Shut down the ZkBackingStore.
     */
    class StopEvent extends AbstractZkBackingStoreEvent<Void> {
        StopEvent() {
            super(false);
        }

        @Override
        public Void execute() {
            if (!started) {
                return null;
            }
            zkClient.unregisterStateChangeHandler(zkStateChangeHandler.name());
            zkClient.unregisterZNodeChangeHandler(controllerChangeHandler.path());
            resignIfActive(true, "The backing store is shutting down.");
            brokerInfo = null;
            activator = null;
            return null;
        }
    }

    /**
     * Handle the ZooKeeper session expiring.
     */
    class SessionExpirationEvent extends AbstractZkBackingStoreEvent<Void> {
        SessionExpirationEvent() {
            super(false);
        }

        @Override
        public Void execute() {
            checkIsStarted();
            resignIfActive(false, "The zookeeper session expired");
            return null;
        }
    }

    /**
     * Handles a change to the /controller znode.
     */
    class ControllerChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        ControllerChangeEvent() {
            super(true);
        }

        @Override
        public Void execute() {
            checkIsStarted();
            int newActiveId = loadControllerId();
            if (newActiveId != activeId) {
                resignIfActive(false, String.format("After reloading %s, the new " +
                    "controller is %d.", controllerChangeHandler.path(), newActiveId));
                activeId = newActiveId;
            }
            return null;
        }

        /**
         * Reload the active controller id from the /controller znode.
         */
        private int loadControllerId() {
            try {
                zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
                return zkClient.getControllerIdAsInt();
            } catch (Throwable e) {
                log.error("Unexpected ZK error in loadControllerId.", e);
                return -1;
            }
        }

    }

    /**
     * Try to become the active controller.
     */
    class TryToActivateEvent extends AbstractZkBackingStoreEvent<Void> {
        private final boolean registerBroker;

        TryToActivateEvent(boolean registerBroker) {
            super(true);
            this.registerBroker = registerBroker;
        }

        @Override
        public Void execute() {
            checkIsStarted();
            if (registerBroker) {
                brokerEpoch = zkClient.registerBroker(brokerInfo);
                log.info("{}: registered brokerInfo {}.", name, brokerInfo);
            }
            KafkaZkClient.RegistrationResult result =
                zkClient.registerControllerAndIncrementControllerEpoch2(nodeId);
            zkClient.registerZNodeChangeHandlerAndCheckExistence(
                controllerChangeHandler);
            activeId = nodeId;
            log.info("{}, {} successfully elected as the controller. Epoch " +
                "incremented to {} and epoch zk version is now {}", name,
                nodeId, result.controllerEpoch(), result.epochZkVersion());
            activeState = loadActiveState(
                result.controllerEpoch(), result.epochZkVersion());
            return null;
        }
    }

    /**
     * Update the current broker information.  Unlike the original broker
     * znode registration, this doesn't change the broker epoch.
     */
    class UpdateBrokerInfoEvent extends AbstractZkBackingStoreEvent<Void> {
        private final BrokerInfo newBrokerInfo;

        UpdateBrokerInfoEvent(BrokerInfo newBrokerInfo) {
            super(false);
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void execute() {
            checkIsStarted(); // We don't need to be active to make this change.
            zkClient.updateBrokerInfo(newBrokerInfo);
            // There's nothing else to do here since, if this is the active controller,
            // the change we just made to the ZK node will trigger the ZK watch on our
            // own broker znode.
            return null;
        }
    }

    /**
     * Deactivate the ZkBackingStore if it is active.
     */
    class DeactivateIfActiveEvent extends AbstractZkBackingStoreEvent<Void> {
        DeactivateIfActiveEvent() {
            super(true);
        }

        @Override
        public Void execute() {
            checkIsStarted();
            resignIfActive(true, "Deactivating the ZkBackingStore.");
            return null;
        }
    }

    class BrokerChildChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        BrokerChildChangeEvent() {
            super(true);
        }

        @Override
        public Void execute() {
            checkIsStartedAndActive();
            // Unfortunately, the ZK watch does not say which child znode was created
            // or deleted, so we have to re-read everything.
            List<MetadataState.Broker> changed = new ArrayList<>();
            MetadataState.BrokerCollection newBrokers = loadBrokerChildren();
            for (MetadataState.Broker newBroker : newBrokers) {
                MetadataState.Broker existingBroker =
                    activeState.metadata.brokers().find(newBroker);
                if (!newBroker.equals(existingBroker)) {
                    if (existingBroker == null) {
                        registerBrokerChangeHandler(newBroker.brokerId());
                    }
                    changed.add(newBroker.duplicate());
                }
            }
            List<Integer> deleted = new ArrayList<>();
            for (MetadataState.Broker existingBroker : activeState.metadata.brokers()) {
                if (newBrokers.find(existingBroker) == null) {
                    deleted.add(existingBroker.brokerId());
                    unregisterBrokerChangeHandler(existingBroker.brokerId());
                }
            }
            activeState.metadata.setBrokers(newBrokers);
            activeState.controller.handleBrokerUpdates(changed, deleted);
            return null;
        }
    }

    class BrokerChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        private final int brokerId;

        BrokerChangeEvent(int brokerId) {
            super(true);
            this.brokerId = brokerId;
        }

        @Override
        public Void execute() {
            checkIsStartedAndActive();
            MetadataState.Broker existingBroker =
                activeState.metadata.brokers().find(brokerId);
            if (existingBroker == null) {
                throw new RuntimeException("Received BrokerChangeEvent for broker " +
                    brokerId + ", but that broker is not in our cached metadata.");
            }
            Optional<BrokerAndEpoch> brokerInfo = OptionConverters.
                toJava(zkClient.getBrokerAndEpoch(brokerId));
            if (!brokerInfo.isPresent()) {
                // The broker znode must have been deleted between the change
                // notification firing and this handler.  Handle the broker going away.
                log.debug("{}: broker {} is now gone.", name, brokerId);
                activeState.metadata.brokers().remove(existingBroker);
                activeState.controller.handleBrokerUpdates(
                    Collections.emptyList(), Collections.singletonList(brokerId));
                return null;
            }
            MetadataState.Broker stateBroker = ControllerUtils.
                brokerToBrokerState(brokerInfo.get().broker());
            stateBroker.setBrokerEpoch(brokerInfo.get().epoch());

            if (!stateBroker.equals(existingBroker)) {
                log.debug("{}: The information for broker {} is now {}",
                    name, brokerId, stateBroker);
                activeState.metadata.brokers().remove(existingBroker);
                activeState.metadata.brokers().add(stateBroker);
                activeState.controller.handleBrokerUpdates(
                    Collections.singletonList(stateBroker.duplicate()),
                    Collections.emptyList());
            } else {
                log.debug("{}: The information for broker {} is unchanged.",
                    name, brokerId);
            }
            return null;
        }
    }

    class TopicChildChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        TopicChildChangeEvent() {
            super(true);
        }

        @Override
        public Void execute() {
            checkIsStartedAndActive();
            // Unfortunately, the ZK watch does not say which child znode was created
            // or deleted, so we have to re-read everything.
            MetadataState.TopicCollection newTopics = loadTopicChildren();
            List<MetadataState.Topic> changed = new ArrayList<>();

            for (MetadataState.Topic newTopic : newTopics) {
                MetadataState.Topic existingTopic =
                    activeState.metadata.topics().find(newTopic);
                if (!newTopic.equals(existingTopic)) {
                    if (existingTopic == null) {
                        registerTopicChangeHandler(newTopic.name());
                    }
                    changed.add(newTopic.duplicate());
                }
            }
            List<String> deleted = new ArrayList<>();
            for (MetadataState.Topic existingTopic : activeState.metadata.topics()) {
                if (newTopics.find(existingTopic) == null) {
                    deleted.add(existingTopic.name());
                    unregisterTopicChangeHandler(existingTopic.name());
                }
            }
            activeState.metadata.setTopics(newTopics);
            activeState.controller.handleTopicUpdates(changed, deleted);
            return null;
        }
    }

    class TopicChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        private final String topic;

        TopicChangeEvent(String topic) {
            super(true);
            this.topic = topic;
        }

        @Override
        public Void execute() {
            checkIsStartedAndActive();
            Map<TopicPartition, ReplicaAssignment> map = CollectionConverters.asJava(
                zkClient.getFullReplicaAssignmentForTopics(
                    CollectionConverters.asScala(Collections.singleton(topic)).toSet()));
            MetadataState.TopicCollection newTopics = ControllerUtils.
                replicaAssignmentsToTopicStates(map);
            MetadataState.Topic newTopic = newTopics.find(topic);
            if (newTopic == null) {
                newTopic = new MetadataState.Topic().setName(topic);
            }
            MetadataState.Topic existingTopic =
                activeState.metadata.topics().find(newTopic);
            if (!newTopic.equals(existingTopic)) {
                activeState.metadata.topics().remove(newTopic);
                activeState.metadata.topics().add(newTopic.duplicate());
                activeState.controller.handleTopicUpdates(
                    Collections.singletonList(newTopic.duplicate()),
                    Collections.emptyList());
            }
            // TODO: add "auto-revert changes to topics in deleting state" here?
            return null;
        }
    }

    private ActiveZkBackingStoreState loadActiveState(int controllerEpoch,
                                                      int epochZkVersion) {
        MetadataState state = new MetadataState();
        zkClient.registerZNodeChildChangeHandler(brokerChildChangeHandler);
        zkClient.registerZNodeChildChangeHandler(topicChildChangeHandler);
        state.setBrokers(loadBrokerChildren());
        log.info("Loaded broker(s) {}", state.brokers());
        state.setTopics(loadTopicChildren());
        log.info("Loaded topic(s) {}", state.topics());
        for (MetadataState.Broker broker : state.brokers()) {
            registerBrokerChangeHandler(broker.brokerId());
        }
        Controller controller = activator.activate(state, controllerEpoch);
        return new ActiveZkBackingStoreState(controllerEpoch,
            epochZkVersion, state, controller);
    }

    private MetadataState.BrokerCollection loadBrokerChildren() {
        MetadataState.BrokerCollection newBrokers =
            new MetadataState.BrokerCollection();
        for (Map.Entry<Broker, Object> entry : CollectionConverters.
            asJava(zkClient.getAllBrokerAndEpochsInCluster()).entrySet()) {
            MetadataState.Broker newBroker = ControllerUtils.
                brokerToBrokerState(entry.getKey());
            newBroker.setBrokerEpoch((Long) entry.getValue());
            newBrokers.add(newBroker);
        }
        return newBrokers;
    }

    private MetadataState.TopicCollection loadTopicChildren() {
        scala.collection.immutable.Set<String> scalaTopics =
            zkClient.getAllTopicsInCluster(true);
        Map<TopicPartition, ReplicaAssignment> map = CollectionConverters.asJava(
            zkClient.getFullReplicaAssignmentForTopics(scalaTopics));
        return ControllerUtils.replicaAssignmentsToTopicStates(map);
    }

    public static ZkBackingStore create(AtomicReference<Throwable> lastUnexpectedError,
                                        int nodeId,
                                        String threadNamePrefix,
                                        KafkaZkClient zkClient) {
        LogContext logContext = new LogContext(String.format("[Node %d] ", nodeId));
        return new ZkBackingStore(lastUnexpectedError,
            logContext.logger(ZkBackingStore.class),
            nodeId,
            new KafkaEventQueue(logContext, threadNamePrefix),
            zkClient);
    }

    ZkBackingStore(AtomicReference<Throwable> lastUnexpectedError,
                   Logger log,
                   int nodeId,
                   EventQueue backingStoreQueue,
                   KafkaZkClient zkClient) {
        this.lastUnexpectedError = lastUnexpectedError;
        Objects.requireNonNull(log);
        this.log = log;
        this.nodeId = nodeId;
        Objects.requireNonNull(backingStoreQueue);
        this.backingStoreQueue = backingStoreQueue;
        Objects.requireNonNull(zkClient);
        this.zkClient = zkClient;
        this.zkStateChangeHandler = new ZkStateChangeHandler();
        this.controllerChangeHandler = new ControllerChangeHandler();
        this.brokerChildChangeHandler = new BrokerChildChangeHandler();
        this.brokerChangeHandlers = new HashMap<>();
        this.topicChildChangeHandler = new TopicChildChangeHandler();
        this.topicChangeHandlers = new HashMap<>();
        this.started = false;
        this.activator = null;
        this.brokerEpoch = -1;
        this.brokerInfo = null;
        this.activeId = -1;
        this.activeState = null;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo,
                                         Activator newActivator) {
        return backingStoreQueue.append(new StartEvent(newBrokerInfo, newActivator));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return backingStoreQueue.append(new UpdateBrokerInfoEvent(newBrokerInfo));
    }

    @Override
    public CompletableFuture<Void> deactivateIfActive() {
        return backingStoreQueue.append(new DeactivateIfActiveEvent());
    }

    @Override
    public void shutdown() {
        log.debug("Shutting down.");
        backingStoreQueue.shutdown(new StopEvent());
    }

    @Override
    public void close() throws InterruptedException {
        log.debug("Initiating close..");
        try {
            shutdown();
        } catch (TimeoutException e) {
            // Ignore duplicate shutdown.
        }
        backingStoreQueue.close();
        log.debug("Close complete.");
    }

    // Visible for testing.
    Throwable lastUnexpectedError() {
        return lastUnexpectedError.get();
    }

    // Visible for testing.
    int nodeId() {
        return nodeId;
    }

    // Visible for testing.
    EventQueue backingStoreQueue() {
        return backingStoreQueue;
    }

    // Visible for testing.
    KafkaZkClient zkClient() {
        return zkClient;
    }

    // Visible for testing.
    MetadataState metadataState() throws ExecutionException, InterruptedException {
        return backingStoreQueue.append(new EventQueue.Event<MetadataState>() {
            @Override
            public MetadataState run() {
                return activeState == null ? null : activeState.metadata.duplicate();
            }
        }).get();
    }
}
