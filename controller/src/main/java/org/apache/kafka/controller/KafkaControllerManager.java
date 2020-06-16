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

import kafka.controller.ControllerManagerFactory;
import kafka.server.KafkaConfig;
import kafka.zk.BrokerInfo;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The KafkaControllerManager is the entry point for communication with the Kafka
 * controller, as well as the container object which owns the components that make up
 * the controller.
 *
 * The Kafka controller handles electing leaders for partitions in the cluster, as well
 * as some other duties.  At any given time, only one node can be the active controller.
 *
 * The controller relies on two other very important components in order to do its job:
 * the BackingStore, and the Propagator.  The BackingStore is responsible for persisting
 * the metadata durably to disk, and also for choosing which node is the active
 * controller.  The Propagator is responsible for sending out updates from the controller
 * to the other nodes in the cluster.
 *
 * Part of the reason why the BackingStore is decoupled from the Controller is to make
 * it possible to use two different metadata stores for Kafka.  One is ZooKeeper; the
 * other is the metadata partition specified by KIP-500.  Therefore, ZooKeeper access is
 * contained entirely in the ZkBackingStore.
 *
 * <pre>
 *                                  KafkaApis
 *                                     ^
 *                                     |
 *                                     V
 *         +--------------+      +------------+       +------------+
 *         |              |      |            |       |            |
 * ZK <--> | BackingStore |<---> | Controller | <---> | Propagator | <--> Cluster
 *         |              |      |  Manager   |       |            |
 *         +--------------+      +------------+       +------------+
 *         `                           ^
 *                                     |
 *                                     V
 *                               +------------+
 *                               |            |
 *                               | Controller |
 *                               |            |
 * </pre>                        +------------+
 *
 * Sometimes, a node will lose the active controllership without realizing it.  In this
 * case, the BackingStore will handle fencing.  Updates from a fenced controller will not
 * take effect, and the node that tried to make them will be notified that it was no
 * longer active.  We also have fencing on the propagation side, in the form of the
 * controller epoch.
 *
 * In the interest of performance, the controller does not write to the BackingStore in
 * a blocking fashion.  Instead, when the controller wants to change something, it first
 * makes the change in its own memory, and then schedules the update to happen in the
 * BackingStore at some time in the future.  This means that the in-memory state of the
 * controller contains uncommitted changes, that have not yet been persisted to disk.
 * However, we do not propagate uncomitted changes to the rest of the cluster.
 *
 * Once KIP-500 is completed, all updates to the BackingStore will go through the active
 * controller.  However, in the meantime, the BackingStore may be updated outside of the
 * active controller, either by other brokers or even by tools external to the cluster.
 * Therefore, when the controller receives an update from the BackingStore, it must check
 * to see if it already applied this update, or not.
 *
 * The Propagator maintains its own cache of the cluster state, as well as what messages
 * need to be sent out to the cluster.  Unlike the controller's cache, the Propagator's
 * cache is not "dirty": it does not contain uncommitted state.
 */
public final class KafkaControllerManager implements ControllerManager {
    private final ControllerLogContext logContext;
    private final KafkaConfig config;
    private final int nodeId;
    private final Time time;
    private final Logger log;
    private final BackingStore backingStore;
    private final Propagator propagator;
    private final EventQueue mainQueue;
    private final PropagationManagerCallbackHandler propagationManagerCallbackHandler;
    private final KafkaBackingStoreCallbackHandler backingStoreCallbackHandler;
    private Controller controller;
    private boolean started;

    class Controller {
        private final int controllerEpoch;
        private final ReplicationManager replicationManager;
        private final PropagationManager propagationManager;

        Controller(int controllerEpoch, MetadataState state) {
            this.controllerEpoch = controllerEpoch;
            this.replicationManager = new ReplicationManager(state);
            this.propagationManager = new PropagationManager(logContext, controllerEpoch, nodeId,
                propagationManagerCallbackHandler, config);
            propagationManager.initialize(replicationManager);
        }

        int controllerEpoch() {
            return controllerEpoch;
        }

        void resign() {
            backingStore.resign(controllerEpoch);
        }

        void handleBrokerUpdates(BrokerDelta delta) {
            long nowNs = time.nanoseconds();
            propagationManager.handleBrokerUpdates(nowNs, delta);
            maybePropagate(nowNs);
        }

        void handleTopicUpdates(TopicDelta delta) {
            long nowNs = time.nanoseconds();
            propagationManager.handleTopicUpdates(nowNs, delta);
            maybePropagate(nowNs);
        }

        void maybePropagate(long nowNs) {
            propagationManager.maybeSendRequests(nowNs, replicationManager, propagator);
            long nextMaybePropagateNs = propagationManager.nextSendTimeNs();
            if (nextMaybePropagateNs != Long.MAX_VALUE) {
                mainQueue.scheduleDeferred("maybePropagate." + controller.controllerEpoch,
                    prevDeadlineNs -> prevDeadlineNs == null ?
                        nextMaybePropagateNs : Math.min(prevDeadlineNs, nextMaybePropagateNs),
                    new MaybePropagateEvent(controller.controllerEpoch));
            }
        }
    }

    abstract class AbstractControllerManagerEvent<T> extends AbstractEvent<T> {
        public AbstractControllerManagerEvent(Optional<Integer> requiredControllerEpoch) {
            super(log, requiredControllerEpoch);
        }

        @Override
        public Optional<Integer> currentControllerEpoch() {
            if (controller == null) {
                return Optional.empty();
            }
            return Optional.of(controller.controllerEpoch());
        }

        @Override
        public Throwable handleException(Throwable e) throws Throwable {
            if (e instanceof ApiException) {
                throw e;
            } else {
                logContext.setLastUnexpectedError(log, "Unhandled event exception", e);
                resignIfActive();
            }
            return e;
        }
    }

    private void resignIfActive() {
        if (controller != null) {
            controller.resign();
            controller = null;
        }
    }

    class KafkaPropagationManagerCallbackHandler implements PropagationManagerCallbackHandler {
        @Override
        public void handleLeaderAndIsrResponse(int brokerId, ClientResponse response) {

        }

        @Override
        public void handleUpdateMetadataResponse(int brokerId, ClientResponse response) {

        }
    }

    class KafkaBackingStoreCallbackHandler implements BackingStoreCallbackHandler {
        @Override
        public void activate(int newControllerEpoch, MetadataState newState) {
            mainQueue.append(new ActivateEvent(newControllerEpoch, newState));
        }

        @Override
        public void deactivate(int controllerEpoch) {
            mainQueue.prepend(new DeactivateEvent(controllerEpoch));
        }

        @Override
        public void handleBrokerUpdates(int controllerEpoch, BrokerDelta delta) {
            mainQueue.append(new HandleBrokerUpdates(controllerEpoch, delta));
        }

        @Override
        public void handleTopicUpdates(int controllerEpoch, TopicDelta delta) {
            mainQueue.append(new HandleTopicUpdates(controllerEpoch, delta));
        }
    }

    class StartEvent extends AbstractControllerManagerEvent<Void> {
        private final BrokerInfo newBrokerInfo;

        public StartEvent(BrokerInfo newBrokerInfo) {
            super(Optional.empty());
            this.newBrokerInfo = newBrokerInfo;
        }

        @Override
        public Void execute() throws Throwable {
            if (started) {
                throw new RuntimeException("Attempting to Start a KafkaControllerManager " +
                    "which has already been started.");
            }
            backingStore.start(newBrokerInfo, backingStoreCallbackHandler).get();
            started = true;
            return null;
        }
    }

    class StopEvent extends AbstractControllerManagerEvent<Void> {
        public StopEvent() {
            super(Optional.empty());
        }

        @Override
        public Void execute() throws Throwable {
            resignIfActive();
            backingStore.shutdown();
            propagator.close();
            backingStore.close();
            return null;
        }
    }

    class ActivateEvent extends AbstractControllerManagerEvent<Void> {
        private final int newControllerEpoch;
        private final MetadataState newState;

        public ActivateEvent(int newControllerEpoch, MetadataState newState) {
            super(Optional.empty());
            this.newControllerEpoch = newControllerEpoch;
            this.newState = newState;
        }

        @Override
        public Void execute() throws Throwable {
            controller = new Controller(newControllerEpoch, newState);
            return null;
        }
    }

    class MaybePropagateEvent extends AbstractControllerManagerEvent<Void> {
        public MaybePropagateEvent(int controllerEpoch) {
            super(Optional.of(controllerEpoch));
        }

        @Override
        public Void execute() throws Throwable {
            controller.maybePropagate(time.nanoseconds());
            return null;
        }
    }

    class DeactivateEvent extends AbstractControllerManagerEvent<Void> {
        public DeactivateEvent(int controllerEpoch) {
            super(Optional.of(controllerEpoch));
        }

        @Override
        public Void execute() throws Throwable {
            controller = null;
            return null;
        }
    }

    class HandleBrokerUpdates extends AbstractControllerManagerEvent<Void> {
        private final BrokerDelta delta;

        public HandleBrokerUpdates(int controllerEpoch, BrokerDelta delta) {
            super(Optional.of(controllerEpoch));
            this.delta = delta;
        }

        @Override
        public Void execute() throws Throwable {
            controller.handleBrokerUpdates(delta);
            return null;
        }
    }

    class HandleTopicUpdates extends AbstractControllerManagerEvent<Void> {
        private final TopicDelta delta;

        public HandleTopicUpdates(int controllerEpoch, TopicDelta delta) {
            super(Optional.of(controllerEpoch));
            this.delta = delta;
        }

        @Override
        public Void execute() throws Throwable {
            controller.handleTopicUpdates(delta);
            return null;
        }
    }

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        ControllerLogContext logContext = 
            new ControllerLogContext(factory.threadNamePrefix(),
                new LogContext(String.format("[Node %d]", factory.nodeId())));
        boolean success = false;
        ZkBackingStore zkBackingStore = null;
        KafkaPropagator kafkaPropagator = null;
        KafkaEventQueue mainQueue = null;
        KafkaControllerManager controllerManager = null;
        try {
            zkBackingStore = ZkBackingStore.create(logContext,
                factory.nodeId(),
                factory.zkClient());
            kafkaPropagator = KafkaPropagator.create(logContext, factory.config(),
                factory.metrics());
            mainQueue = new KafkaEventQueue(new LogContext(
                logContext.logContext().logPrefix() + " [mainQueue] "),
                logContext.threadNamePrefix());
            controllerManager = new KafkaControllerManager(logContext, factory.config(),
                factory.nodeId(), SystemTime.SYSTEM, zkBackingStore,
                kafkaPropagator, mainQueue);
            success = true;
        } finally {
            if (!success) {
                Utils.closeQuietly(zkBackingStore, "zkBackingStore");
                Utils.closeQuietly(kafkaPropagator, "kafkaPropagator");
                Utils.closeQuietly(mainQueue, "mainQueue");
            }
        }
        return controllerManager;
    }

    KafkaControllerManager(ControllerLogContext logContext,
                           KafkaConfig config,
                           int nodeId,
                           Time time,
                           BackingStore backingStore,
                           Propagator propagator,
                           EventQueue mainQueue) {
        this.logContext = logContext;
        this.config = config;
        this.nodeId = nodeId;
        this.time = time;
        this.log = logContext.createLogger(KafkaControllerManager.class);
        this.backingStore = backingStore;
        this.propagator = propagator;
        this.mainQueue = mainQueue;
        this.propagationManagerCallbackHandler =
            new KafkaPropagationManagerCallbackHandler();
        this.backingStoreCallbackHandler = new KafkaBackingStoreCallbackHandler();
        this.controller = null;
        this.started = false;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo) {
        return mainQueue.append(new StartEvent(newBrokerInfo));
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return backingStore.updateBrokerInfo(newBrokerInfo);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
            electLeaders(int timeoutMs, Set<TopicPartition> parts) {
        return null;
    }

    @Override
    public CompletableFuture<Set<TopicPartition>> controlledShutdown(int brokerId,
                                                                     int brokerEpoch) {
        return null;
    }

    @Override
    public void shutdown() {
        log.debug("Shutting down.");
        mainQueue.shutdown(new StopEvent());
    }

    @Override
    public void close() throws Exception {
        log.debug("Initiating close.");
        shutdown();
        mainQueue.close();
        log.debug("Close complete.");
    }
}
