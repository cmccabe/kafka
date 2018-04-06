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

package org.apache.kafka.soak.action;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.action.ActionGraph.ActionData;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Runs roles for cluster nodes.
 */
public final class ActionScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ActionScheduler.class);

    public static final Pattern DEFAULT_ACTION_FILTER = Pattern.compile(".*");

    public static final class Builder {
        private final SoakCluster cluster;
        private final List<Action> actions = new ArrayList<>();
        private Pattern actionFilter = DEFAULT_ACTION_FILTER;
        private List<String> roots = new ArrayList<>();

        public Builder(SoakCluster cluster) {
            this.cluster = cluster;
        }

        public Builder addAction(Action action) {
            actions.add(action);
            return this;
        }

        public Builder actionFilter(Pattern actionFilter) {
            this.actionFilter = actionFilter;
            return this;
        }

        public Builder addRoot(String root) {
            roots.add(root);
            return this;
        }

        public ActionScheduler build() throws Exception {
            ActionGraph actionTree = new ActionGraph.Builder().
                nodeNames(cluster.nodes().keySet()).
                addActions(actions).
                addRoots(roots).
                build();
            log.info("WATERMELON: actionTree contains: {}", Utils.join(actionTree.actionMap().keySet(), ", "));
            for (Map.Entry<ActionId, ActionData> entry : actionTree.actionMap().entrySet()) {
                log.info("{} -> (id={}, action={}, comesBefore={})", entry.getKey(), entry.getValue().action().id(), entry.getValue().action(), Utils.join(entry.getValue().comesBefore(), ", "));
            }
            ActionScheduler scheduler = new ActionScheduler(cluster, actionFilter, actionTree);
            return scheduler;
        }
    }

    private final static class SchedulerNode {
        /**
         * The name of this node.
         */
        private final String nodeName;

        /**
         * The executor service which executes tasks for this node.
         */
        private final ExecutorService executorService;

        /**
         * Actions which are able to run on this node, but which haven't run yet.
         */
        private final HashSet<ActionId> pending = new HashSet<>();

        /**
         * A description of what is currently going on on this node.
         */
        private String description = "";

        SchedulerNode(String nodeName) throws IOException {
            this.nodeName = nodeName;
            this.executorService = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("SoakThread_" + nodeName, false));
        }
    }

    private final class SchedulerNodeTask implements Runnable {
        private final SchedulerNode node;
        private final ActionId id;

        SchedulerNodeTask(SchedulerNode node, ActionId id) {
            this.node = node;
            this.id = id;
        }

        @Override
        public void run() {
            final ActionData actionData = actionTree.actionMap().get(id);
            final Action action = actionData.action();
            try {
                SoakNode soakNode;
                boolean shouldCall;
                synchronized (ActionScheduler.this) {
                    node.description = action.id().toString();
                    soakNode = cluster.nodes().get(node.nodeName);
                    if (actionFilter.matcher(action.toString()).matches()) {
                        SoakLog.printToAll(String.format("*** Running %s%n", action.toString()),
                            cluster.clusterLog(), soakNode.log());
                        shouldCall = true;
                    } else {
                        SoakLog.printToAll(String.format("*** Skipping %s because it doesn't " +
                                "match pattern: %s%n", action.id().toString(), actionFilter.toString()),
                            cluster.clusterLog(), soakNode.log());
                        shouldCall = false;
                    }
                }
                if (shouldCall) {
                    action.call(cluster, soakNode);
                }
                boolean done = false;
                SoakLog.printToAll(String.format("*** WATERMELON Finished Running %s%n", action.toString()),
                    cluster.clusterLog(), soakNode.log());
                synchronized (ActionScheduler.this) {
                    node.description = "";
                    for (ActionId id : actionData.comesBefore()) {
                        runIfPossible(id);
                    }
                    pendingActions--;
                    if (pendingActions == 0) {
                        done = true;
                    }
                }
                if (done) {
                    shutdownFuture.complete(null);
                }
            } catch (Throwable t) {
                RuntimeException e =
                    new RuntimeException(action.id().toString() + " failed", t);
                shutdownFuture.completeExceptionally(e);
                node.executorService.shutdownNow();
                throw e;
            }
        }
    }

    /**
     * The cluster configuration.
     */
    private final SoakCluster cluster;

    /**
     * The pattern to use to filter actions.
     */
    private final Pattern actionFilter;

    /**
     * The actions to run.
     */
    private final ActionGraph actionTree;

    /**
     * The scheduler nodes.
     */
    private final Map<String, SchedulerNode> nodes;

    /**
     * A future which will be completed when all actions are done.
     */
    private final KafkaFutureImpl<Void> shutdownFuture = new KafkaFutureImpl<>();

    /**
     * The number of actions which are still pending.
     */
    private int pendingActions = 0;

    private ActionScheduler(SoakCluster cluster, Pattern actionFilter, ActionGraph actionTree) throws Exception {
        this.cluster = cluster;
        this.actionFilter = actionFilter;
        this.actionTree = actionTree;
        this.nodes = new HashMap<>();
        for (SoakNode node : cluster.nodes().values()) {
            this.nodes.put(node.nodeName(), new SchedulerNode(node.nodeName()));
        }
        log.info("WATERMELON: actionMap size = {}, concreteRoots size = {}",
            actionTree.actionMap().size(), actionTree.concreteRoots().size());
        for (ActionData actionData : actionTree.actionMap().values()) {
            ActionId id = actionData.action().id();
            SchedulerNode node = nodes.get(id.scope());
            node.pending.add(id);
            log.info("WATERMLEON: id={}", id);
            pendingActions++;
        }
        for (ActionId id : actionTree.concreteRoots()) {
            runIfPossible(id);
        }
        if (pendingActions == 0) {
            shutdownFuture.complete(null);
        }
    }

    private synchronized void runIfPossible(ActionId id) {
        SchedulerNode node = nodes.get(id.scope());
        if (node == null) {
            throw new RuntimeException("Action " + id + " references non-existent node " + id.scope());
        }
        if (!node.pending.contains(id)) {
            return;
        }
        Action action = actionTree.actionMap().get(id).action();
        for (ActionId afterId : action.comesAfter()) {
            if (isPending(afterId)) {
                return;
            }
        }
        node.pending.remove(id);
        node.executorService.submit(new SchedulerNodeTask(node, id));
    }

    private synchronized boolean isPending(ActionId id) {
        ActionData actionData = actionTree.actionMap().get(id);
        if (actionData == null) {
            return false;
        }
        SchedulerNode node = nodes.get(id.scope());
        if (node == null) {
            throw new RuntimeException("Action " + id + " references non-existent node " + id.scope());
        }
        return node.pending.contains(id);
    }

    @Override
    public void close() {
        List<ExecutorService> executorServices = new ArrayList<>();
        synchronized (this) {
            for (SchedulerNode node : nodes.values()) {
                node.executorService.shutdownNow();
                executorServices.add(node.executorService);
            }
        }
        for (ExecutorService executorService : executorServices) {
            SoakUtil.awaitTerminationUninterruptibly(executorService);
        }
        shutdownFuture.completeExceptionally(new RuntimeException("The ActionScheduler was closed."));
    }

    public void await(long waitTimeMs) throws Throwable {
        try {
            shutdownFuture.get(waitTimeMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            StringBuilder bld = new StringBuilder();
            synchronized (this) {
                String prefix = "";
                for (SchedulerNode node : nodes.values()) {
                    bld.append(prefix).append(node.description);
                    prefix = ", ";
                }
            }
            throw new RuntimeException("Timed out while waiting for node(s): " + bld.toString(), e);
        } catch (ExecutionException e) {
            log.info("ActionScheduler#await error {}", Utils.fullStackTrace(e.getCause()));
            throw e.getCause();
        } finally {
            Utils.closeQuietly(this, "ActionScheduler");
        }
    }
}
