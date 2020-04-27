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
import kafka.zk.BrokerIdZNode;
import kafka.zk.BrokerInfo;
import kafka.zk.BrokersZNode;
import kafka.zk.ControllerZNode;
import kafka.zk.KafkaZkClient;
import kafka.zk.KafkaZkClient.BrokerAndEpoch;
import kafka.zk.StateChangeHandlers;
import kafka.zookeeper.StateChangeHandler;
import kafka.zookeeper.ZNodeChangeHandler;
import kafka.zookeeper.ZNodeChildChangeHandler;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.MetadataStateData;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ZkBackingStore implements BackingStore {
    private final Logger log;
    private final AtomicReference<Throwable> lastUnexpectedError;
    private final int nodeId;
    private final EventQueue eventQueue;
    private final KafkaZkClient zkClient;
    private final ZkStateChangeHandler zkStateChangeHandler;
    private final ControllerChangeHandler controllerChangeHandler;
    private final BrokerChildChangeHandler brokerChildChangeHandler;
    private final Map<Integer, BrokerChangeHandler> brokerChangeHandlers;
    private boolean started;
    private ChangeListener changeListener;
    private long brokerEpoch;
    private BrokerInfo brokerInfo;
    private int activeId;
    private int controllerEpoch;
    private int epochZkVersion;
    private MetadataStateData state;

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
            NotControllerException exception =
                new NotControllerException("Zookeeper session expired");
            CompletableFuture<Void> future = eventQueue.clearAndEnqueue(exception,
                new SessionExpirationEvent());
            ControllerUtils.await(log, future);
            changeListener.deactivate();
        }

        @Override
        public void afterInitializingSession() {
            eventQueue.append(new TryToActivateEvent(true));
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
            eventQueue.append(new ControllerChangeEvent());
        }

        @Override
        public void handleDeletion() {
            eventQueue.append(new TryToActivateEvent(false));
        }

        @Override
        public void handleDataChange() {
            eventQueue.append(new ControllerChangeEvent());
        }
    }

    /**
     * Handles changes to the children of the /broker path.
     */
    class BrokerChildChangeHandler implements ZNodeChildChangeHandler {
        @Override
        public String path() {
            return BrokersZNode.path();
        }

        @Override
        public void handleChildChange() {
            eventQueue.append(new BrokerChildChangeEvent());
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
            eventQueue.append(new BrokerChangeEvent(brokerId));
        }
    }

    /**
     * Reload the active controller id from the /controller znode.
     * Resign if the new active controller is another broker.
     */
    private void reloadControllerZnodeAndResignIfNeeded() {
        int newActiveID = -1;
        try {
            zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
            newActiveID = zkClient.getControllerIdAsInt();
        } catch (Throwable e) {
            log.error("Unexpected ZK error in reloadControllerZnodeAndResignIfNeeded.", e);
        }
        if (nodeId == activeId) {
            if (newActiveID == activeId) {
                resign(true, "the controller znode has been modified");
            } else {
                resign(false, String.format("after reloading %s, we learned that the new " +
                    "controller is %d", controllerChangeHandler.path(), newActiveID));
                activeId = newActiveID;
            }
        } else {
            activeId = newActiveID;
            log.info("After reloading {}, we learned that the new controller is {}.",
                controllerChangeHandler.path(), newActiveID);
        }
    }

    /**
     * Transition away from being the current active controller.
     *
     * @param deleteZnode   True if we should delete the controller znode.
     * @param reason        The reason for the transition.
     */
    private void resign(boolean deleteZnode, String reason) {
        if (activeId != nodeId) {
            throw new RuntimeException("Can't resign because this node is not active.");
        }
        log.warn("Resigning because {}.", reason);
        state = null;
        try {
            zkClient.unregisterZNodeChildChangeHandler(brokerChildChangeHandler.path());
            for (Iterator<BrokerChangeHandler> iter =
                 brokerChangeHandlers.values().iterator(); iter.hasNext(); ) {
                zkClient.unregisterZNodeChangeHandler(iter.next().path());
                iter.remove();
            }
            changeListener.deactivate();
        } catch (Throwable e) {
            log.error("Unable to unregister ZkClient watches.", e);
            lastUnexpectedError.set(e);
        }
        if (deleteZnode) {
            try {
                zkClient.deleteController(epochZkVersion);
            } catch (ControllerMovedException e) {
                log.info("Tried to delete {} during resignation, but it has already "+
                    "been modified.", ControllerZNode.path());
            } catch (Throwable e) {
                log.error("Unable to delete {} during resignation due to unexpected " +
                    "error.", e);
                lastUnexpectedError.set(e);
            }
        }
        activeId = -1;
        controllerEpoch = -1;
        epochZkVersion = -1;
    }

    private static final int NONE = 0;
    private static final int REQUIRE_STARTED = 1 << 0;
    private static final int REQUIRE_ACTIVE = 1 << 1;
    private static final int INFO_LEVEL_LOG = 1 << 2;
    private static final int HANDLE_CME = 1 << 3;

    abstract class BaseZkBackingStoreEvent<T> implements EventQueue.Event<T> {
        final String name;
        private final int flags;

        BaseZkBackingStoreEvent(int flags) {
            this.name = getClass().getSimpleName();
            this.flags = flags;
        }

        private boolean hasFlag(int f) {
            return (flags & f) != 0;
        }

        @Override
        final public T run() {
            long startNs = Time.SYSTEM.nanoseconds();
            if (hasFlag(INFO_LEVEL_LOG)) {
                log.info("{}: starting", name);
            } else if (log.isDebugEnabled()) {
                log.debug("{}: starting", name);
            }
            try {
                if (hasFlag(REQUIRE_STARTED) && !started) {
                    throw new RuntimeException("The ZkBackingStore has not been started.");
                }
                if (hasFlag(REQUIRE_ACTIVE) && nodeId != activeId) {
                    throw new ControllerMovedException("This node is not the " +
                        "active controller.");
                }
                T value = execute();
                if (hasFlag(INFO_LEVEL_LOG)) {
                    log.info("{}: finished after {} ms",
                        name, executionTimeToString(startNs));
                } else if (log.isDebugEnabled()) {
                    log.debug("{}: finished after {} ms",
                        name, executionTimeToString(startNs));
                }
                return value;
            } catch (Throwable e) {
                if (hasFlag(HANDLE_CME) && e instanceof ControllerMovedException) {
                    log.info("{}: caught ControllerMovedException after {} ms.  " +
                        "Reloading controller znode.", name,
                        executionTimeToString(startNs));
                    reloadControllerZnodeAndResignIfNeeded();
                } else {
                    log.error("{}: finished after {} ms with unexpected error", name,
                        executionTimeToString(startNs), e);
                    lastUnexpectedError.set(e);
                    resign(true, "the controller had an unexpected error");
                }
            } finally {
                long endNs = Time.SYSTEM.nanoseconds();
                if (hasFlag(INFO_LEVEL_LOG)) {
                    log.info("{}: finished after {} ms", name,
                        ControllerUtils.nanosToFractionalMillis(endNs - startNs));
                } else if (log.isDebugEnabled()) {
                    log.debug("{}: finished after {} ms", name,
                        ControllerUtils.nanosToFractionalMillis(endNs - startNs));
                }
            }
            return null;
        }

        final private String executionTimeToString(long startNs) {
            long endNs = Time.SYSTEM.nanoseconds();
            return ControllerUtils.nanosToFractionalMillis(endNs - startNs);
        }

        public abstract T execute();
    }

    /**
     * Initialize the ZkBackingStore.
     */
    class StartEvent extends BaseZkBackingStoreEvent<Void> {
        private final BrokerInfo newBrokerInfo;
        private final ChangeListener newChangeListener;

        StartEvent(BrokerInfo newBrokerInfo, ChangeListener newChangeListener) {
            super(INFO_LEVEL_LOG);
            this.newBrokerInfo = newBrokerInfo;
            this.newChangeListener = newChangeListener;
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
            eventQueue.append(new TryToActivateEvent(false));
            changeListener = newChangeListener;
            started = true;
            log.info("{}: initialized ZkBackingStore with brokerInfo {}.",
                name, newBrokerInfo);
            return null;
        }
    }

    /**
     * Shut down the ZkBackingStore.
     */
    class StopEvent extends BaseZkBackingStoreEvent<Void> {
        StopEvent() {
            super(REQUIRE_STARTED | INFO_LEVEL_LOG);
        }

        @Override
        public Void execute() {
            zkClient.unregisterStateChangeHandler(zkStateChangeHandler.name());
            zkClient.unregisterZNodeChangeHandler(controllerChangeHandler.path());
            resign(true, "the backing store is shutting down");
            brokerInfo = null;
            changeListener = null;
            return null;
        }
    }

    /**
     * Handle the ZooKeeper session expiring.
     */
    class SessionExpirationEvent extends BaseZkBackingStoreEvent<Void> {
        SessionExpirationEvent() {
            super(REQUIRE_STARTED | INFO_LEVEL_LOG);
        }

        @Override
        public Void execute() {
            resign(false, "the zookeeper session expired");
            return null;
        }
    }

    /**
     * Handles a change to the /controller znode.
     */
    class ControllerChangeEvent extends BaseZkBackingStoreEvent<Void> {
        ControllerChangeEvent() {
            super(REQUIRE_STARTED | INFO_LEVEL_LOG);
        }

        @Override
        public Void execute() {
            reloadControllerZnodeAndResignIfNeeded();
            return null;
        }
    }

    /**
     * Try to become the active controller.
     */
    class TryToActivateEvent extends BaseZkBackingStoreEvent<Void> {
        private final boolean registerBroker;

        TryToActivateEvent(boolean registerBroker) {
            super(REQUIRE_STARTED | INFO_LEVEL_LOG | HANDLE_CME);
            this.registerBroker = registerBroker;
        }

        @Override
        public Void execute() {
            if (registerBroker) {
                brokerEpoch = zkClient.registerBroker(brokerInfo);
                log.info("{}: registered brokerInfo {}.", name, brokerInfo);
            }
            KafkaZkClient.RegistrationResult result =
                zkClient.registerControllerAndIncrementControllerEpoch2(nodeId);
            zkClient.registerZNodeChangeHandlerAndCheckExistence(
                controllerChangeHandler);
            activeId = nodeId;
            controllerEpoch = result.controllerEpoch();
            epochZkVersion = result.epochZkVersion();
            log.info("{}, {} successfully elected as the controller. Epoch " +
                "incremented to {} and epoch zk version is now {}", name,
                nodeId, controllerEpoch, epochZkVersion);
            loadZkState();
            return null;
        }
    }

    private void loadZkState() {
        this.state = new MetadataStateData();
        zkClient.registerZNodeChildChangeHandler(brokerChildChangeHandler);
        state.setBrokers(loadBrokerChildren());
        for (MetadataStateData.Broker broker : state.brokers()) {
            registerBrokerChangeHandler(broker.brokerId());
        }
        changeListener.activate(state.duplicate());
    }

    private void registerBrokerChangeHandler(int brokerId) {
        BrokerChangeHandler changeHandler = new BrokerChangeHandler(brokerId);
        zkClient.registerZNodeChangeHandlerAndCheckExistence(changeHandler);
        brokerChangeHandlers.put(brokerId, changeHandler);
    }

    private MetadataStateData.BrokerCollection loadBrokerChildren() {
        MetadataStateData.BrokerCollection newBrokers =
            new MetadataStateData.BrokerCollection();
        for (Map.Entry<Broker, Object> entry : CollectionConverters.
                asJava(zkClient.getAllBrokerAndEpochsInCluster()).entrySet()) {
            MetadataStateData.Broker newBroker = ControllerUtils.
                brokerToStateBroker(entry.getKey());
            newBroker.setBrokerEpoch((Long) entry.getValue());
        }
        return newBrokers;
    }

    /**
     * Update the current broker information.  Unlike the original broker
     * znode registration, this doesn't change the broker epoch.
     */
    class UpdateBrokerInfoEvent extends BaseZkBackingStoreEvent<Void> {
        private final BrokerInfo newBrokerInfo;

        UpdateBrokerInfoEvent(BrokerInfo newBrokerInfo) {
            super(REQUIRE_STARTED);
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void execute() {
            zkClient.updateBrokerInfo(newBrokerInfo);
            // UpdateBrokerEvent may be triggered even if this node is not
            // the active controller.  If it is the active controller, our change
            // to the ZK node will trigger the ZK watch on our own broker znode.
            return null;
        }
    }

    class BrokerChildChangeEvent extends BaseZkBackingStoreEvent<Void> {
        BrokerChildChangeEvent() {
            super(REQUIRE_STARTED | REQUIRE_ACTIVE | HANDLE_CME);
        }

        @Override
        public Void execute() {
            // Unfortunately, the ZK watch does not say which child znode was created
            // or deleted, so we have to re-read everything.
            List<MetadataStateData.Broker> changed = new ArrayList<>();
            MetadataStateData.BrokerCollection newBrokers = loadBrokerChildren();
            for (MetadataStateData.Broker newBroker : newBrokers) {
                MetadataStateData.Broker existingBroker = state.brokers().find(newBroker);
                if (!newBroker.equals(existingBroker)) {
                    changed.add(newBroker.duplicate());
                }
            }
            List<Integer> deleted = new ArrayList<>();
            for (MetadataStateData.Broker existingBroker : state.brokers()) {
                if (newBrokers.find(existingBroker) == null) {
                    deleted.add(existingBroker.brokerId());
                }
            }
            state.setBrokers(newBrokers);
            changeListener.handleBrokerUpdates(changed, deleted);
            return null;
        }
    }

    class BrokerChangeEvent extends BaseZkBackingStoreEvent<Void> {
        private final int brokerId;

        BrokerChangeEvent(int brokerId) {
            super(REQUIRE_STARTED | REQUIRE_ACTIVE | HANDLE_CME);
            this.brokerId = brokerId;
        }

        @Override
        public Void execute() {
            MetadataStateData.Broker existingBroker = state.brokers().find(brokerId);
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
                state.brokers().remove(existingBroker);
                changeListener.handleBrokerUpdates(
                    Collections.emptyList(), Collections.singletonList(brokerId));
                return null;
            }
            MetadataStateData.Broker stateBroker = ControllerUtils.
                brokerToStateBroker(brokerInfo.get().broker());
            stateBroker.setBrokerEpoch(brokerInfo.get().epoch());

            if (!stateBroker.equals(existingBroker)) {
                log.debug("{}: The information for broker {} is now {}",
                    name, brokerId, stateBroker);
                changeListener.handleBrokerUpdates(
                    Collections.singletonList(stateBroker), Collections.emptyList());
            } else {
                log.debug("{}: The information for broker {} is unchanged.",
                    name, brokerId);
            }
            return null;
        }
    }

    public static ZkBackingStore create(int nodeId,
                                        String threadNamePrefix,
                                        KafkaZkClient zkClient) {
        LogContext logContext = new LogContext(String.format("[Node %d] ", nodeId));
        return new ZkBackingStore(logContext.logger(ZkBackingStore.class),
            nodeId,
            new KafkaEventQueue(logContext, threadNamePrefix),
            zkClient);
    }

    ZkBackingStore(Logger log,
                   int nodeId,
                   EventQueue eventQueue,
                   KafkaZkClient zkClient) {
        Objects.requireNonNull(log);
        this.log = log;
        this.lastUnexpectedError = new AtomicReference<>(null);
        this.nodeId = nodeId;
        Objects.requireNonNull(eventQueue);
        this.eventQueue = eventQueue;
        Objects.requireNonNull(zkClient);
        this.zkClient = zkClient;
        this.zkStateChangeHandler = new ZkStateChangeHandler();
        this.controllerChangeHandler = new ControllerChangeHandler();
        this.brokerChildChangeHandler = new BrokerChildChangeHandler();
        this.brokerChangeHandlers = new HashMap<>();
        this.started = false;
        this.changeListener = null;
        this.brokerEpoch = -1;
        this.brokerInfo = null;
        this.activeId = -1;
        this.controllerEpoch = -1;
        this.epochZkVersion = -1;
        this.state = null;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo,
                                         ChangeListener newChangeListener) {
        return eventQueue.append(new StartEvent(newBrokerInfo, newChangeListener));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return eventQueue.append(new UpdateBrokerInfoEvent(newBrokerInfo));
    }

    @Override
    public void shutdown(TimeUnit timeUnit, long timeSpan) {
        log.debug("Shutting down with a timeout of {} {}.", timeSpan, timeUnit);
        eventQueue.shutdown(new StopEvent(), timeUnit, timeSpan);
    }

    @Override
    public void close() throws InterruptedException {
        log.debug("Initiating close..");
        try {
            shutdown(TimeUnit.DAYS, 0);
        } catch (TimeoutException e) {
            // Ignore duplicate shutdown.
        }
        eventQueue.close();
        log.debug("Close complete.");
    }

    // Visible for testing.
    int nodeId() {
        return nodeId;
    }

    // Visible for testing.
    EventQueue eventQueue() {
        return eventQueue;
    }

    // Visible for testing.
    KafkaZkClient zkClient() {
        return zkClient;
    }

    // Visible for testing.
    Throwable lastUnexpectedError() {
        return lastUnexpectedError.get();
    }
}
