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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.LogContext;
import kafka.controller.ControllerManager;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class KafkaControllerManager implements ControllerManager {
    private final String threadNamePrefix;
    private final LogContext logContext;
    private final Logger log;
    private final AtomicReference<Throwable> lastUnexpectedError;
    private final BackingStore backingStore;
    private final KafkaActivator activator;
    private final ReentrantLock lock;
    private KafkaController controller;
    private boolean started;
    private boolean shutdown;

    private <T> CompletableFuture<T> makeControllerCall(
        Function<KafkaController, CompletableFuture<T>> fun) {
        lock.lock();
        try {
            checkStartedAndNotShutdown();
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

    private void checkStartedAndNotShutdown() {
        lock.lock();
        try {
            if (!started) {
                throw new RuntimeException("The ControllerManager was never started.");
            }
            if (shutdown) {
                throw new TimeoutException("The ControllerManager has been shut down.");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
        backingStore.close();
    }

    public boolean isShutdown() {
        lock.lock();
        try {
            return shutdown;
        } finally {
            lock.unlock();
        }
    }

    class KafkaActivator implements BackingStore.Activator {
        @Override
        public BackingStore.Controller activate(MetadataState state, int epoch) {
            KafkaController newController = new KafkaController(logContext,
                threadNamePrefix, backingStore, state, epoch);
            lock.lock();
            try {
                checkStartedAndNotShutdown();
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

    public static KafkaControllerManager create(ControllerManagerFactory factory) {
        LogContext logContext =
            new LogContext(String.format("[Node %d]", factory.nodeId()));
        AtomicReference<Throwable> lastUnexpectedError = new AtomicReference<>(null);
        ZkBackingStore zkBackingStore = ZkBackingStore.create(lastUnexpectedError,
                factory.nodeId(),
                factory.threadNamePrefix(),
                factory.zkClient());
        return new KafkaControllerManager(factory.threadNamePrefix(),
                logContext,
                lastUnexpectedError,
                zkBackingStore);
    }

    KafkaControllerManager(String threadNamePrefix,
                           LogContext logContext,
                           AtomicReference<Throwable> lastUnexpectedError,
                           BackingStore backingStore) {
        this.threadNamePrefix = threadNamePrefix;
        this.logContext = logContext;
        this.log = logContext.logger(KafkaControllerManager.class);
        this.lastUnexpectedError = lastUnexpectedError;
        this.backingStore = backingStore;
        this.activator = new KafkaActivator();
        this.lock = new ReentrantLock();
        this.controller = null;
        this.started = false;
    }

    @Override
    public CompletableFuture<Void> start(BrokerInfo newBrokerInfo) {
        lock.lock();
        try {
            if (started) {
                throw new RuntimeException("The ControllerManager has already been started.");
            }
            if (shutdown) {
                throw new TimeoutException("The ControllerManager has been shut down.");
            }
            started = true;
            return backingStore.start(newBrokerInfo, activator).exceptionally(
                new Function<Throwable, Void>() {
                    @Override
                    public Void apply(Throwable e) {
                        log.warn("Unable to start BackingStore.  Shutting down.", e);
                        lock.lock();
                        try {
                            shutdown = true;
                        } finally {
                            lock.unlock();
                        }
                        lastUnexpectedError.set(e);
                        return null;
                    }
                });
        } catch (Exception e) {
            lastUnexpectedError.set(e);
            return ControllerUtils.exceptionalFuture(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            if (shutdown)
                return;
            shutdown = true;
        } finally {
            lock.unlock();
        }
        backingStore.shutdown();
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
}
