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
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class KafkaControllerManager implements ControllerManager {
    private final ControllerLogContext logContext;
    private final Logger log;
    private final BackingStore backingStore;
    private final KafkaActivator activator;
    private final KafkaDeactivator deactivator;
    private final ReentrantLock lock;
    private KafkaController controller;
    private State state;

    enum State {
        NOT_STARTED,
        RUNNING,
        SHUT_DOWN
    }

    private <T> CompletableFuture<T> makeControllerCall(
        Function<KafkaController, CompletableFuture<T>> fun) {
        lock.lock();
        try {
            checkRunning();
            if (controller == null) {
                throw new NotControllerException("This node is not the controller.");
            }
            return fun.apply(controller);
        } catch (Exception e) {
            return ControllerUtils.exceptionalFuture(e);
        } finally {
            lock.unlock();
        }
    }

    private void checkRunning() {
        lock.lock();
        try {
            if (state != State.RUNNING) {
                throw new RuntimeException("The ControllerManager is not running.");
            }
        } finally {
            lock.unlock();
        }
    }

    State state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    class KafkaActivator implements Activator {
        @Override
        public Controller activate(MetadataState state, int epoch) {
            KafkaController newController = new KafkaController(
                    logContext, deactivator, backingStore, state, epoch);
            lock.lock();
            try {
                checkRunning();
                controller = newController;
            } catch (Exception e) {
                newController.close();
                throw e;
            } finally {
                lock.unlock();
            }
            return newController;
        }
    }

    class KafkaDeactivator implements Deactivator {
        @Override
        public void deactivate(Controller oldController) {
            lock.lock();
            try {
                if (controller == oldController) {
                    controller = null;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        ControllerLogContext logContext = 
            new ControllerLogContext(factory.threadNamePrefix(),
                new LogContext(String.format("[Node %d]", factory.nodeId())));
        ZkBackingStore zkBackingStore = ZkBackingStore.create(logContext,
                factory.nodeId(),
                factory.zkClient());
        return new KafkaControllerManager(logContext, zkBackingStore);
    }

    KafkaControllerManager(ControllerLogContext logContext,
                           BackingStore backingStore) {
        this.logContext = logContext;
        this.log = logContext.createLogger(KafkaControllerManager.class);
        this.backingStore = backingStore;
        this.activator = new KafkaActivator();
        this.deactivator = new KafkaDeactivator();
        this.lock = new ReentrantLock();
        this.controller = null;
        this.state = State.NOT_STARTED;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo) {
        lock.lock();
        try {
            if (state != State.NOT_STARTED) {
                throw new RuntimeException("The ControllerManager has already been started.");
            }
            state = State.RUNNING;
        } catch (Exception e) {
            logContext.setLastUnexpectedError(log, "error in when starting controller " +
                "manager", e);
            return ControllerUtils.exceptionalFuture(e);
        } finally {
            lock.unlock();
        }
        return backingStore.start(newBrokerInfo, activator).exceptionally(
            new Function<Throwable, Void>() {
                @Override
                public Void apply(Throwable e) {
                    log.warn("Unable to start BackingStore. Shutting down.", e);
                    lock.lock();
                    try {
                        state = State.SHUT_DOWN;
                    } finally {
                        lock.unlock();
                    }
                    logContext.setLastUnexpectedError(log, "error in when starting " +
                        "controller manager", e);
                    throw new RuntimeException(e);
                }
            });
    }

    @Override
    public CompletableFuture<Void> updateBrokerInfo(BrokerInfo newBrokerInfo) {
        return backingStore.updateBrokerInfo(newBrokerInfo);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
            electLeaders(int timeoutMs, Set<TopicPartition> parts) {
        return makeControllerCall(
            controller -> controller.electLeaders(timeoutMs, parts));
    }

    @Override
    public CompletableFuture<Set<TopicPartition>> controlledShutdown(int brokerId,
                                                                     int brokerEpoch) {
        return null;
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            if (state == State.SHUT_DOWN) return;
            state = State.SHUT_DOWN;
        } finally {
            lock.unlock();
        }
        backingStore.shutdown();
    }

    @Override
    public void close() throws Exception {
        shutdown();
        backingStore.close();
    }
}
