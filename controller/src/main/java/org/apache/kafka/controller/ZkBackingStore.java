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
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.controller.ReplicaAssignment;
import kafka.zk.BrokerIdZNode;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.BrokerInfo;
import kafka.zk.ControllerZNode;
import kafka.zk.IsrChangeNotificationZNode;
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
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import scala.collection.Seq;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ZkBackingStore implements BackingStore {
    private final ControllerLogContext logContext;
    private final Logger log;
    private final int nodeId;
    private final EventQueue backingStoreQueue;
    private final KafkaZkClient zkClient;
    private final ZkStateChangeHandler zkStateChangeHandler;
    private final ControllerChangeHandler controllerChangeHandler;
    private final Map<String, ZNodeChildChangeHandler> childChangeHandlers;
    private final Map<Integer, BrokerChangeHandler> brokerChangeHandlers;
    private final Map<String, TopicChangeHandler> topicChangeHandlers;
    private boolean started;
    private BackingStoreCallbackHandler callbackHandler;
    private long brokerEpoch;
    private BrokerInfo brokerInfo;
    private int activeId;
    private ActiveZkBackingStoreState activeState;

    static class ActiveZkBackingStoreState {
        final int controllerEpoch;
        final int epochZkVersion;
        final MetadataState metadata;

        ActiveZkBackingStoreState(int controllerEpoch,
                                  int epochZkVersion,
                                  MetadataState metadata) {
            this.controllerEpoch = controllerEpoch;
            this.epochZkVersion = epochZkVersion;
            this.metadata = metadata;
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
            // Prepend the SessionExpirationEvent to the queue, so that it will be
            // handled as soon as possible.
            CompletableFuture<Void> future = backingStoreQueue.prepend(
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
        private final int controllerEpoch;

        BrokerChildChangeHandler(int controllerEpoch) {
            this.controllerEpoch = controllerEpoch;
        }

        @Override
        public String path() {
            return BrokerIdsZNode.path();
        }

        @Override
        public void handleChildChange() {
            backingStoreQueue.append(new BrokerChildChangeEvent(controllerEpoch));
        }
    }

    /**
     * Handles changes to a specific broker ZNode /broker/$ID
     */
    class BrokerChangeHandler implements ZNodeChangeHandler {
        private final int controllerEpoch;
        private final int brokerId;

        BrokerChangeHandler(int controllerEpoch, int brokerId) {
            this.controllerEpoch = controllerEpoch;
            this.brokerId = brokerId;
        }

        @Override
        public String path() {
            return BrokerIdZNode.path(brokerId);
        }

        @Override
        public void handleDataChange() {
            backingStoreQueue.append(new BrokerChangeEvent(controllerEpoch, brokerId));
        }
    }

    /**
     * Handles changes to the children of the /broker/topics path.
     */
    class TopicChildChangeHandler implements ZNodeChildChangeHandler {
        private final int controllerEpoch;

        TopicChildChangeHandler(int controllerEpoch) {
            this.controllerEpoch = controllerEpoch;
        }

        @Override
        public String path() {
            return TopicsZNode.path();
        }

        @Override
        public void handleChildChange() {
            backingStoreQueue.append(new TopicChildChangeEvent(controllerEpoch));
        }
    }

    /**
     * Handles changes to a /broker/topics/$topic path.
     */
    class TopicChangeHandler implements ZNodeChangeHandler {
        private final int controllerEpoch;
        private final String topic;

        TopicChangeHandler(int controllerEpoch, String topic) {
            this.controllerEpoch = controllerEpoch;
            this.topic = topic;
        }

        @Override
        public String path() {
            return TopicZNode.path(topic);
        }

        @Override
        public void handleDataChange() {
            backingStoreQueue.append(new TopicChangeEvent(controllerEpoch, topic));
        }
    }

    class IsrChangeNotificationChildHandler implements ZNodeChildChangeHandler {
        private final int controllerEpoch;

        IsrChangeNotificationChildHandler(int controllerEpoch) {
            this.controllerEpoch = controllerEpoch;
        }

        @Override
        public String path() {
            return IsrChangeNotificationZNode.path();
        }

        @Override
        public void handleChildChange() {
            backingStoreQueue.append(new IsrChangeNotificationChildEvent(controllerEpoch));
        }
    }

    private void registerZnodeChildChangeHandler(ZNodeChildChangeHandler handler) {
        childChangeHandlers.put(handler.path(), handler);
        zkClient.registerZNodeChildChangeHandler(handler);
    }

    private void registerBrokerChangeHandler(int controllerEpoch, int brokerId) {
        BrokerChangeHandler changeHandler =
            new BrokerChangeHandler(controllerEpoch, brokerId);
        zkClient.registerZNodeChangeHandlerAndCheckExistence(changeHandler);
        brokerChangeHandlers.put(brokerId, changeHandler);
    }

    private void unregisterBrokerChangeHandler(int brokerId) {
        BrokerChangeHandler handler = brokerChangeHandlers.remove(brokerId);
        if (handler != null) {
            zkClient.unregisterZNodeChangeHandler(handler.path());
        }
    }

    private void registerTopicChangeHandler(int controllerEpoch, String name) {
        TopicChangeHandler changeHandler =
            new TopicChangeHandler(controllerEpoch, name);
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
        public AbstractZkBackingStoreEvent(Optional<Integer> controllerEpoch) {
            super(log, controllerEpoch);
        }

        @Override
        public Optional<Integer> currentControllerEpoch() {
            if (activeState == null) {
                return Optional.empty();
            }
            return Optional.of(activeState.controllerEpoch);
        }

        @Override
        public Throwable handleException(Throwable e) throws Throwable {
            if (e instanceof NotControllerException) {
                if (!controllerEpochMismatch()) {
                    // If the controller epoch that this event was prepared for is
                    // different than the current one, we don't want to resign.  In
                    // theory, we could be active again with a different epoch.
                    resignIfActive(true, "The controller got a NotControllerException.");
                }
                return e;
            } else if (e instanceof ControllerMovedException) {
                // ControllerMovedException is thrown by some of the ZK fencing routines
                // inside ZkBackingStore.
                resignIfActive(true, "The controller got a ControllerMovedException.");
                return new NotControllerException(e.getMessage());
            } else {
                // Other exception types are not expected by ZkBackingStore.
                logContext.setLastUnexpectedError(log, "Unhandled event exception", e);
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
            for (Iterator<String> iter = childChangeHandlers.keySet().iterator();
                     iter.hasNext(); ) {
                String path = iter.next();
                zkClient.unregisterZNodeChildChangeHandler(path);
                iter.remove();
            }
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
            callbackHandler.deactivate(activeState.controllerEpoch);
        } catch (Throwable e) {
            logContext.setLastUnexpectedError(log, "unable to unregister ZkClient " +
                    "watches", e);
        }
        if (deleteZnode) {
            try {
                zkClient.deleteController(activeState.epochZkVersion);
            } catch (ControllerMovedException e) {
                log.info("Tried to delete {} during resignation, but it has already " +
                    "been modified.", ControllerZNode.path());
            } catch (Throwable e) {
                logContext.setLastUnexpectedError(log, "unable to unregister ZkClient " +
                        "watches during controller resignation", e);
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
        private final BackingStoreCallbackHandler newCallbackHandler;

        StartEvent(BrokerInfo newBrokerInfo, BackingStoreCallbackHandler newCallbackHandler) {
            super(Optional.empty());
            this.newBrokerInfo = newBrokerInfo;
            this.newCallbackHandler = newCallbackHandler;
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
            callbackHandler = newCallbackHandler;
            started = true;
            log.info("{}: initialized ZkBackingStore with brokerInfo {}.",
                name(), newBrokerInfo);
            return null;
        }
    }

    /**
     * Shut down the ZkBackingStore.
     */
    class StopEvent extends AbstractZkBackingStoreEvent<Void> {
        StopEvent() {
            super(Optional.empty());
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
            callbackHandler = null;
            return null;
        }
    }

    /**
     * Handle the ZooKeeper session expiring.
     */
    class SessionExpirationEvent extends AbstractZkBackingStoreEvent<Void> {
        SessionExpirationEvent() {
            super(Optional.empty());
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
            super(Optional.empty());
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
            super(Optional.empty());
            this.registerBroker = registerBroker;
        }

        @Override
        public Void execute() {
            checkIsStarted();
            if (registerBroker) {
                brokerEpoch = zkClient.registerBroker(brokerInfo);
                log.info("{}: registered brokerInfo {}.", name(), brokerInfo);
            }
            // This will throw ControllerMovedException if we don't get the leadership.
            KafkaZkClient.RegistrationResult result =
                zkClient.registerControllerAndIncrementControllerEpoch2(nodeId);
            zkClient.registerZNodeChangeHandlerAndCheckExistence(
                controllerChangeHandler);
            activeId = nodeId;
            log.info("{}, {} successfully elected as the controller. Epoch " +
                "incremented to {} and epoch zk version is now {}", name(),
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
            super(Optional.empty());
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
     * Resign as controller.
     */
    class ResignEvent extends AbstractZkBackingStoreEvent<Void> {
        ResignEvent(int requiredControllerEpoch) {
            super(Optional.of(requiredControllerEpoch));
        }

        @Override
        public Void execute() {
            checkIsStarted();
            resignIfActive(true, "Deactivating the ZkBackingStore.");
            return null;
        }
    }

    /**
     * Handle a change to a znode directly under /brokers
     */
    class BrokerChildChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        BrokerChildChangeEvent(int requiredControllerEpoch) {
            super(Optional.of(requiredControllerEpoch));
        }

        @Override
        public Void execute() {
            checkIsStarted();
            // Unfortunately, the ZK watch does not say which child znode was created
            // or deleted, so we have to re-read everything.
            List<MetadataState.Broker> changed = new ArrayList<>();
            MetadataState.BrokerCollection newBrokers = loadBrokerChildren();
            for (MetadataState.Broker newBroker : newBrokers) {
                MetadataState.Broker existingBroker =
                    activeState.metadata.brokers().find(newBroker);
                if (!newBroker.equals(existingBroker)) {
                    if (existingBroker == null) {
                        registerBrokerChangeHandler(activeState.controllerEpoch,
                                newBroker.brokerId());
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
            callbackHandler.handleBrokerUpdates(activeState.controllerEpoch,
                new BrokerDelta(changed, deleted));
            return null;
        }
    }

    class BrokerChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        private final int brokerId;

        BrokerChangeEvent(int controllerEpoch, int brokerId) {
            super(Optional.of(controllerEpoch));
            this.brokerId = brokerId;
        }

        @Override
        public Void execute() {
            checkIsStarted();
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
                log.debug("{}: broker {} is now gone.", name(), brokerId);
                activeState.metadata.brokers().remove(existingBroker);
                callbackHandler.handleBrokerUpdates(activeState.controllerEpoch,
                    new BrokerDelta(Collections.emptyList(),
                        Collections.singletonList(brokerId)));
                return null;
            }
            MetadataState.Broker stateBroker = ControllerUtils.
                brokerToBrokerState(brokerInfo.get().broker());
            stateBroker.setBrokerEpoch(brokerInfo.get().epoch());

            if (!stateBroker.equals(existingBroker)) {
                log.debug("{}: The information for broker {} is now {}",
                    name(), brokerId, stateBroker);
                activeState.metadata.brokers().remove(existingBroker);
                activeState.metadata.brokers().add(stateBroker);
                callbackHandler.handleBrokerUpdates(activeState.controllerEpoch,
                    new BrokerDelta(Collections.singletonList(stateBroker.duplicate()),
                        Collections.emptyList()));
            } else {
                log.debug("{}: The information for broker {} is unchanged.",
                    name(), brokerId);
            }
            return null;
        }
    }

    class TopicChildChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        TopicChildChangeEvent(int controllerEpoch) {
            super(Optional.of(controllerEpoch));
        }

        @Override
        public Void execute() {
            checkIsStarted();
            // If a new znode is created under /brokers/topics, or a znode is deleted
            // from under /brokers/topics, we will execute this code. Unfortunately, ZK
            // doesn't tell us which znode(s) were created or deleted, only that some
            // change happened.  And of course, additional changes may happen in between
            // the ZK watch triggering and our processing here.  So the end result is
            // that we have to rescan every znode directly under /brokers/topics here.
            //
            // Topic creation uses this code path.  Someone creates the topic znode, and
            // then this code triggers.  Later on, we will create the ISR znodes which
            // are in /brokers/topics/<topic_name>/partitions/<partition>/state.
            // We don't expect them to be present for a newly created topic-- we don't
            // even check for them here, in fact.
            //
            // The last step for topic deletion also runs through here (but not the
            // initial steps...)
            //
            // Finally, we may find some other arbitrary change to a topic znode.
            // Normally these would be handled in the individual topic znode watcher
            // callback.  However, this callback may get there first by chance.
            // There are two main changes that can happen here: adding partitions,
            // or changing the replica set.  (Removing partitions is not really
            // supported by Kafka right now, but the code is here anyway...)
            MetadataState.TopicCollection newTopics = loadTopicChildren();
            TopicDelta delta = TopicDelta.fromUpdatedTopicReplicas(
                activeState.metadata.topics(), newTopics);
            applyDelta(requiredControllerEpoch().get(), delta);
            callbackHandler.handleTopicUpdates(activeState.controllerEpoch, delta);
            return null;
        }
    }

    private void applyDelta(int controllerEpoch, TopicDelta delta) {
        for (MetadataState.Topic addedTopic : delta.addedTopics) {
            registerTopicChangeHandler(controllerEpoch, addedTopic.name());
        }
        for (String removedTopic : delta.removedTopics) {
            unregisterTopicChangeHandler(removedTopic);
        }
        // For most partitions, we don't watch the ISR znode.
        // But in case we are, stop doing that when the partition goes away.
//        for (TopicPartition removedPartition : removedParts) {
//            unregisterPartitionStateChangeHandler(...);
//        }
        delta.apply(activeState.metadata.topics());
    }

    class TopicChangeEvent extends AbstractZkBackingStoreEvent<Void> {
        private final String topic;

        TopicChangeEvent(int controllerEpoch, String topic) {
            super(Optional.of(controllerEpoch));
            this.topic = topic;
        }

        @Override
        public Void execute() {
            checkIsStarted();
            // This is a lot like the handler for TopicChildChangeEvent, execept that
            // we have been notified about a change to a specific topic znode, rather than
            // told to look at every topic znode in existence to see if any got added or
            // removed.  So we can be more efficient.
            Map<TopicPartition, ReplicaAssignment> map = CollectionConverters.asJava(
                    zkClient.getFullReplicaAssignmentForTopics(
                    CollectionConverters.asScala(Collections.singleton(topic)).toSet()));
            MetadataState.TopicCollection updatedTopics = ControllerUtils.
                replicaAssignmentsToTopicStates(map);
            MetadataState.Topic updatedTopic = updatedTopics.find(topic);
            TopicDelta delta;
            if (updatedTopic == null) {
                // In theory the znode could have been deleted between the watch getting
                // triggered and this code executing.  So we have to be prepared for that.
                delta = TopicDelta.fromSingleTopicRemoval(topic);
            } else {
                delta = TopicDelta.fromUpdatedTopicReplicas(
                    activeState.metadata.topics(), updatedTopic);
            }
            applyDelta(requiredControllerEpoch().get(), delta);
            callbackHandler.handleTopicUpdates(activeState.controllerEpoch, delta);
            return null;
        }
    }

    private ActiveZkBackingStoreState loadActiveState(int controllerEpoch,
                                                      int epochZkVersion) {
        MetadataState state = new MetadataState();
        registerZnodeChildChangeHandler(new BrokerChildChangeHandler(controllerEpoch));
        registerZnodeChildChangeHandler(new TopicChildChangeHandler(controllerEpoch));
        registerZnodeChildChangeHandler(
            new IsrChangeNotificationChildHandler(controllerEpoch));
        state.setBrokers(loadBrokerChildren());
        log.info("Loaded broker(s) {}", state.brokers());
        state.setTopics(loadTopicChildren());
        for (MetadataState.Topic topic : state.topics()) {
            registerTopicChangeHandler(controllerEpoch, topic.name());
        }
        zkClient.deleteIsrChangeNotifications(epochZkVersion);
        mergeIsrInformation(state.topics());
        log.info("Loaded topic(s) {}", state.topics());
        for (MetadataState.Broker broker : state.brokers()) {
            registerBrokerChangeHandler(controllerEpoch, broker.brokerId());
        }
        callbackHandler.activate(controllerEpoch, state);
        return new ActiveZkBackingStoreState(controllerEpoch, epochZkVersion, state);
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

    private void mergeIsrInformation(MetadataState.TopicCollection topics) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (MetadataState.Topic topic : topics) {
            for (MetadataState.Partition partition : topic.partitions()) {
                partitions.add(new TopicPartition(topic.name(), partition.id()));
            }
        }
        Map<TopicPartition, LeaderIsrAndControllerEpoch> isrUpdates =
            CollectionConverters.asJava(zkClient.getTopicPartitionStates(
                CollectionConverters.asScala(partitions)));
        TopicDelta delta = TopicDelta.fromIsrUpdates(topics, isrUpdates);
        delta.apply(topics);
    }

    class IsrChangeNotificationChildEvent extends AbstractZkBackingStoreEvent<Void> {
        IsrChangeNotificationChildEvent(int controllerEpoch) {
            super(Optional.of(controllerEpoch));
        }

        @Override
        public Void execute() {
            checkIsStarted();
            // The in-sync replica set of a partition is stored in
            // /brokers/topics/<topic_name>/partitions/<partition>/state.
            // However, there are too many partition state znodes to set up a separate
            // ZooKeeper watch on each individual one, like we do with topics.
            // So, instead, we create sort of a pub/sub system within Zookeeper.
            // The way that it works is that the leader of a partition changes a
            // partition state znode, and then writes a new entry underneath
            // /isr_change_notification.  The entry tells us what partition state znode
            // to re-read.
            Seq<String> sequenceNumbers = zkClient.getAllIsrChangeNotifications();
            try {
                Seq<TopicPartition> parts =
                    zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers);
                // Load the partitions into a set to remove duplicates
                HashSet<TopicPartition> topicParts = new HashSet<>();
                for (Iterator<TopicPartition> iter =
                         CollectionConverters.asJava(parts.iterator()); iter.hasNext(); ) {
                    topicParts.add(iter.next());
                }
                Map<TopicPartition, LeaderIsrAndControllerEpoch> isrUpdates =
                    CollectionConverters.asJava(zkClient.getTopicPartitionStates(
                        CollectionConverters.asScala(topicParts.iterator()).toSeq()));
                TopicDelta delta = TopicDelta.
                    fromIsrUpdates(activeState.metadata.topics(), isrUpdates);
                applyDelta(requiredControllerEpoch().get(), delta);
                callbackHandler.handleTopicUpdates(activeState.controllerEpoch, delta);
            } finally {
                // delete the notifications
                zkClient.deleteIsrChangeNotifications(sequenceNumbers,
                    activeState.epochZkVersion);
            }
            return null;
        }
    }

    public static ZkBackingStore create(ControllerLogContext logContext,
                                        int nodeId,
                                        KafkaZkClient zkClient) {
        return new ZkBackingStore(logContext, nodeId,
            new KafkaEventQueue(new LogContext(logContext.logContext().logPrefix() +
                " [storeQueue] "), logContext.threadNamePrefix()),
            zkClient);
    }

    ZkBackingStore(ControllerLogContext logContext,
                   int nodeId,
                   EventQueue backingStoreQueue,
                   KafkaZkClient zkClient) {
        this.logContext = logContext;
        this.log = logContext.createLogger(ZkBackingStore.class);
        this.nodeId = nodeId;
        this.backingStoreQueue = backingStoreQueue;
        this.zkClient = zkClient;
        this.zkStateChangeHandler = new ZkStateChangeHandler();
        this.controllerChangeHandler = new ControllerChangeHandler();
        this.childChangeHandlers = new HashMap<>();
        this.brokerChangeHandlers = new HashMap<>();
        this.topicChangeHandlers = new HashMap<>();
        this.started = false;
        this.callbackHandler = null;
        this.brokerEpoch = -1;
        this.brokerInfo = null;
        this.activeId = -1;
        this.activeState = null;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo,
                                         BackingStoreCallbackHandler newCallbackHandler) {
        return backingStoreQueue.append(new StartEvent(newBrokerInfo, newCallbackHandler));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return backingStoreQueue.append(new UpdateBrokerInfoEvent(newBrokerInfo));
    }

    @Override
    public CompletableFuture<Void> resign(int controllerEpoch) {
        return backingStoreQueue.append(new ResignEvent(controllerEpoch));
    }

    @Override
    public void beginShutdown() {
        log.debug("Shutting down.");
        backingStoreQueue.beginShutdown(new StopEvent());
    }

    @Override
    public void close() throws InterruptedException {
        log.debug("Initiating close.");
        beginShutdown();
        backingStoreQueue.close();
        log.debug("Close complete.");
    }

    // Visible for testing.
    ControllerLogContext logContext() {
        return logContext;
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
