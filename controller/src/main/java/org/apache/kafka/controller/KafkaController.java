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

import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
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
public final class KafkaController {
    private final ControllerLogContext logContext;
    private final Logger log;
    private final int controllerEpoch;
    private final BackingStore backingStore;
    private final EventQueue mainQueue;
    private final MetadataState state;

    KafkaController(ControllerLogContext logContext,
                    int controllerEpoch,
                    BackingStore backingStore,
                    EventQueue mainQueue,
                    MetadataState state) {
        this.logContext = logContext;
        this.log = new LogContext(logContext.logContext().logPrefix() +
            " [epoch " + controllerEpoch + "] ").logger(KafkaController.class);
        this.controllerEpoch = controllerEpoch;
        this.backingStore = backingStore;
        this.mainQueue = mainQueue;
        this.state = state;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }
}
