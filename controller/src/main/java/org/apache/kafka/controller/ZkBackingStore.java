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
import kafka.zk.ControllerZNode;
import kafka.zk.KafkaZkClient;
import kafka.zk.StateChangeHandlers;
import kafka.zookeeper.StateChangeHandler;
import kafka.zookeeper.ZNodeChangeHandler;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ZkBackingStore implements BackingStore {
    private final Logger log;
    private final int nodeId;
    private final EventQueue eventQueue;
    private final KafkaZkClient zkClient;
    private final ZkStateChangeHandler zkStateChangeHandler;
    private final ControllerChangeHandler controllerChangeHandler;
    private boolean started;
    private ActivationListener activationListener;
    private long brokerEpoch;
    private BrokerInfo brokerInfo;
    private int activeId;
    private int controllerEpoch;
    private int epochZkVersion;

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
            activationListener.deactivate();
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
     * Verify that the ZkBackingStore has been started.
     */
    void verifyRunning() {
        if (!started)
            throw new RuntimeException("The ZkBackingStore has not been started.");
    }

    /**
     * Returns true if this node is the active controller.
     */
    private boolean isActive() {
        return nodeId == activeId;
    }

    /**
     * Throws an exception unless this node is the active controller.
     */
    private void verifyActive()  {
        if (!isActive())
            throw new ControllerMovedException("This node is not the active controller.");
    }

    /**
     * Update the current active controller.
     * If that update de-activates this controller, then resign.
     */
    private void maybeResign() {
        if (shouldResign())
            resign();
    }

    private boolean shouldResign() {
        boolean wasActive = isActive();
        try {
            zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
            activeId = zkClient.getControllerIdAsInt();
            return wasActive && !isActive();
        } catch (Throwable e) {
            log.error("Unhandled error in shouldResign.", e);
            activeId = -1;
            return true;
        }
    }

    private void resign() {
        try {
            log.info("Resigning");
            controllerEpoch = -1;
            epochZkVersion = -1;
//        zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path);
//        zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path);
//        zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path);
//        zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path);
//        unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet);
//        // de-register partition ISR listener for on-going partition reassignment task
//        unregisterPartitionReassignmentIsrChangeHandlers();
//        zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path);
//        unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq);
//        zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path);
//        zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path);
            activationListener.deactivate();
        } catch (Throwable e) {
            log.error("Unhandled error in resign.", e);
        }
    }

    class EventContext implements Closeable {
        private final String name;
        private final long startNs;
        private long endNs;
        private boolean closed;

        EventContext(String name) {
            this.name = name;
            this.startNs = Time.SYSTEM.nanoseconds();
            this.closed = false;
            if (log.isDebugEnabled()) {
                log.debug("{}: starting.", name);
            }
        }

        @Override
        public String toString() {
            return name;
        }

        public void close(Throwable e, boolean stackTrace) {
            if (closed)
                return;
            closed = true;
            this.endNs = Time.SYSTEM.nanoseconds();
            if (log.isDebugEnabled()) {
                log.debug("{}: finished after {} ns.", name, endNs - startNs);
            }
            if (e != null) {
                if (stackTrace) {
                    log.info("{}: failed.", name, endNs - startNs, e);
                } else {
                    log.info("{}: failed with error {}.", name, endNs - startNs, e.getMessage());
                }
            }
        }

        @Override
        public void close() {
            close(null, false);
        }
    }

    /**
     * Initialize the ZkBackingStore.
     */
    class StartEvent implements EventQueue.Event<Void> {
        private final BrokerInfo newBrokerInfo;
        private final ActivationListener newActivationListener;

        StartEvent(BrokerInfo newBrokerInfo, ActivationListener newActivationListener) {
            this.newBrokerInfo = newBrokerInfo;
            this.newActivationListener = newActivationListener;
        }

        @Override
        public Void run() {
            EventContext context = new EventContext("StartEvent");
            try {
                if (started) {
                    throw new RuntimeException("Attempting to Start a BackingStore " +
                        "which has already been started.");
                }
                zkClient.registerStateChangeHandler(zkStateChangeHandler);
                brokerEpoch = zkClient.registerBroker(brokerInfo);
                zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
                brokerInfo = newBrokerInfo;
                eventQueue.append(new TryToActivateEvent(false));
                activationListener = newActivationListener;
                started = true;
                log.info("{}: initialized ZkBackingStore with brokerInfo {}.", context, newBrokerInfo);
            } catch (Throwable e) {
                context.close(e, true);
                throw e;
            } finally {
                context.close();
            }
            return null;
        }
    }

    /**
     * Shut down the ZkBackingStore.
     */
    class StopEvent implements EventQueue.Event<Void> {
        @Override
        public Void run() {
            EventContext context = new EventContext("StopEvent");
            try {
                zkClient.unregisterStateChangeHandler(zkStateChangeHandler.name());
                zkClient.unregisterZNodeChangeHandler(controllerChangeHandler.path());
                maybeResign();
                brokerInfo = null;
                activationListener = null;
                log.info("{}: stopped ZkBackingStore.");
            } catch (Throwable e) {
                context.close(e, true);
                throw e;
            } finally {
                context.close();
            }
            return null;
        }
    }

    /**
     * Handle the ZooKeeper session expiring.
     */
    class SessionExpirationEvent implements EventQueue.Event<Void> {
        @Override
        public Void run() {
            EventContext context = new EventContext("SessionExpirationEvent");
            try {
                boolean wasActive = isActive();
                activeId = -1;
                if (wasActive)
                    resign();
            } finally {
                context.close();
            }
            return null;
        }
    }

    /**
     * Handles a change to the /controller znode.
     */
    class ControllerChangeEvent implements EventQueue.Event<Void> {
        @Override
        public Void run() {
            EventContext context = new EventContext("ControllerChangeEvent");
            try {
                maybeResign();
            } finally {
                context.close();
            }
            return null;
        }
    }

    /**
     * Try to become the active controller.
     */
    class TryToActivateEvent implements EventQueue.Event<Void> {
        private final boolean registerBroker;

        TryToActivateEvent(boolean registerBroker) {
            this.registerBroker = registerBroker;
        }

        @Override
        public Void run() {
            EventContext context = new EventContext("TryToActivateEvent");
            try {
                if (registerBroker) {
                    brokerEpoch = zkClient.registerBroker(brokerInfo);
                }
                int curActiveId = zkClient.getControllerIdAsInt();
                if (curActiveId == -1) {
                    KafkaZkClient.RegistrationResult result =
                        zkClient.registerControllerAndIncrementControllerEpoch2(nodeId);
                    activeId = nodeId;
                    controllerEpoch = result.controllerEpoch();
                    epochZkVersion = result.epochZkVersion();
                    log.warn("{}, {} successfully elected as the controller. Epoch " +
                        "incremented to {} and epoch zk version is now {}", context,
                        nodeId, controllerEpoch, epochZkVersion);
                    loadZkState();
                } else if (curActiveId != nodeId) {
                    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler);
                    activeId = curActiveId;
                    controllerEpoch = -1;
                    epochZkVersion = -1;
                    log.info("{}: Broker {} has been elected as the controller, so " +
                        "stopping the election process.", context, curActiveId);
                }
            } catch (ControllerMovedException e) {
                log.info("{}: caught ControllerMovedException while trying to active.",
                    context);
                maybeResign();
            } catch (Throwable e) {
                context.close(e, true);
            } finally {
                context.close();
            }
            return null;
        }

        private void loadZkState() {
            activationListener.activate(null);
        }
    }

    /**
     * Update the current broker information.  Unlike the original broker
     * znode registration, this doesn't change the broker epoch.
     */
    class UpdateBrokerInfoEvent implements EventQueue.Event<Void> {
        private final BrokerInfo newBrokerInfo;

        UpdateBrokerInfoEvent(BrokerInfo newBrokerInfo) {
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void run() {
            EventContext context = new EventContext("UpdateBrokerInfoEvent");
            try {
                zkClient.updateBrokerInfo(newBrokerInfo);
            } catch (Throwable e) {
                context.close(e, true);
            } finally {
                context.close();
            }
            return null;
        }
    }

    public static ZkBackingStore create(int nodeId,
                                        String threadNamePrefix,
                                        KafkaZkClient zkClient) {
        LogContext logContext = new LogContext(String.format("[Node %d]", nodeId));
        return new ZkBackingStore(logContext.logger(ZkBackingStore.class),
            nodeId,
            new KafkaEventQueue(logContext, threadNamePrefix),
            zkClient);
    }

    ZkBackingStore(Logger log,
                   int nodeId,
                   EventQueue eventQueue,
                   KafkaZkClient zkClient) {
        this.log = log;
        this.nodeId = nodeId;
        this.eventQueue = eventQueue;
        this.zkClient = zkClient;
        this.zkStateChangeHandler = new ZkStateChangeHandler();
        this.controllerChangeHandler = new ControllerChangeHandler();
        this.started = false;
        this.activationListener = null;
        this.brokerEpoch = -1;
        this.brokerInfo = null;
        this.activeId = -1;
        this.controllerEpoch = -1;
        this.epochZkVersion = -1;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo,
                                         ActivationListener newActivationListener) {
        return eventQueue.append(new StartEvent(newBrokerInfo, newActivationListener));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return eventQueue.append(new UpdateBrokerInfoEvent(newBrokerInfo));
    }

    @Override
    public void shutdown(TimeUnit timeUnit, long timeSpan) {
        eventQueue.shutdown(new StopEvent(), timeUnit, timeSpan);
    }

    @Override
    public void close() throws InterruptedException {
        shutdown(TimeUnit.DAYS, 0);
        eventQueue.close();
    }
}
