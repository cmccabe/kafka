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

package org.apache.kafka.soak.role;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
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

    private static final class SchedulerNode {
        /**
         * The name of this node.
         */
        private final String nodeName;

        /**
         * The executor service which executes tasks for this node.
         */
        private final ExecutorService executorService;

        /**
         * A description of what is currently going on on this node.
         */
        private String description;

        SchedulerNode(String nodeName) throws IOException {
            this.nodeName = nodeName;
            this.executorService = Executors.newSingleThreadScheduledExecutor(
                    ThreadUtils.createThreadFactory("SoakThread_" + nodeName, false));
            this.description = "";
        }
    }

    public static class Builder {
        private final SoakCluster cluster;
        private final List<Action> actions = new ArrayList<>();
        private Pattern actionFilter = DEFAULT_ACTION_FILTER;

        private UnresolvedDependencyHandler dependencyHandler = new UnresolvedDependencyHandler() {
            @Override
            public Action resolve(Dependency dep, Collection<String> clusterNodeNames)
                    throws UnsatisfiedDependencyException {
                throw new UnsatisfiedDependencyException(dep);
            }
        };

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

        public Builder dependencyHandler(UnresolvedDependencyHandler dependencyHandler) {
            this.dependencyHandler = dependencyHandler;
            return this;
        }

        public ActionScheduler build() throws Exception {
            return new ActionScheduler(cluster, actions, dependencyHandler, actionFilter);
        }
    }

    /**
     * The cluster configuration.
     */
    private final SoakCluster cluster;

    /**
     * A future which will be completed when all actions are done.
     */
    private final KafkaFutureImpl<Void> shutdownFuture = new KafkaFutureImpl<>();

    /**
     * The scheduler nodes.
     */
    private final TreeMap<String, SchedulerNode> nodes = new TreeMap<>();

    /**
     * A map from dependencies to the actions waiting for them.
     */
    private final TreeMap<Dependency, Set<Action>> waiters = new TreeMap<>(new Comparator<Dependency>() {
        @Override
        public int compare(Dependency a, Dependency b) {
            int cmp = a.id().compareTo(b.id());
            if (cmp != 0) {
                return cmp;
            }
            cmp = a.scope().compareTo(b.scope());
            if (cmp != 0) {
                return cmp;
            }
            return 0;
        }
    });

    /**
     * Dependencies which have already been provided.
     */
    private final HashSet<Dependency> provided;

    /**
     * The number of actions which are still pending.
     */
    private int pendingActions;

    /**
     * The pattern to use to filter actions.
     */
    private final Pattern actionFilter;

    public ActionScheduler(SoakCluster cluster, List<Action> actions,
            UnresolvedDependencyHandler dependencyHandler, Pattern actionFilter) throws Exception {
        this.cluster = cluster;
        this.pendingActions = actions.size();
        this.provided = new HashSet<>();
        try {
            for (String depString : cluster.provided()) {
                this.provided.add(Dependency.fromString(depString));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error processing provided dependencies", e);
        }
        this.actionFilter = actionFilter;
        boolean success = false;
        try {
            for (String nodeName : cluster.nodes().keySet()) {
                this.nodes.put(nodeName, new SchedulerNode(nodeName));
            }
            for (Action action : actions) {
                registerActionProvides(action);
            }
            for (Dependency dep : provided) {
                waiters.put(dep, new HashSet<Action>());
            }
            for (int actionIndex = 0; actionIndex < actions.size(); actionIndex++) {
                Action action = actions.get(actionIndex);
                for (Requirement requirement : action.requirements()) {
                    Dependency dep = requirement.dependency();
                    Set<Action> waiting = waiters.get(dep);
                    if (waiting == null) {
                        if (requirement.hard()) {
                            Action newAction = dependencyHandler.resolve(dep, cluster.nodes().keySet());
                            if (newAction == null) {
                                log.trace("{}: ignoring missing hard dependency {}", action, dep);
                                this.waiters.put(dep, new HashSet<Action>());
                                this.provided.add(dep);
                            } else {
                                log.trace("{}: created new action {} to provide {}", action, newAction, dep);
                                actions.add(newAction);
                                registerActionProvides(newAction);
                                pendingActions++;
                            }
                        } else {
                            log.trace("{}: ignoring missing soft dependency {}", action, dep);
                            this.waiters.put(dep, new HashSet<Action>());
                            this.provided.add(dep);
                        }
                    }
                }
            }
            for (Action action : actions) {
                for (Requirement requirement : action.requirements()) {
                    waiters.get(requirement.dependency()).add(action);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Scheduling {} action(s):", actions.size());
                for (Action action : actions) {
                    log.debug("action {}: requires {}", action.toString(),
                        Utils.join(Arrays.asList(action.requirements()), ", "));
                }
            }
            for (Action action : actions) {
                if (canSchedule(action)) {
                    schedule(action);
                }
            }
            if (actions.isEmpty()) {
                shutdownFuture.complete(null);
            }
            success = true;
        } finally {
            if (!success) {
                close();
            }
        }
    }

    private void registerActionProvides(Action action) {
        for (String depId : action.provides()) {
            Dependency.validateId(depId);
            waiters.put(new Dependency(depId, action.nodeName()), new HashSet<Action>());
            waiters.put(new Dependency(depId, "all"), new HashSet<Action>());
        }
    }

    /**
     * Handles unresolved dependency problems.
     */
    public interface UnresolvedDependencyHandler {
        public Action resolve(Dependency dep, Collection<String> clusterNodeNames)
                throws UnsatisfiedDependencyException;
    }

    /**
     * An exception indicating that a dependency could not be satisfied.
     */
    public static class UnsatisfiedDependencyException extends RuntimeException {
        private final Dependency dep;

        public UnsatisfiedDependencyException(Dependency dep) {
            super("Unsatisfied dependency " + dep);
            this.dep = dep;
        }

        public Dependency dep() {
            return dep;
        }
    }

    /**
     * Returns true if there is an entry in the pending map for actionName
     * besides actionName:all.
     */
    private synchronized boolean otherNodesPending(String actionName) {
        Dependency key = new Dependency(actionName, "");
        while (true) {
            key = waiters.higherKey(key);
            if (key == null) {
                // There are no more entries in the waiters map.  Therefore,
                // there are no other entries for pending nodes.
                return false;
            }
            if (!key.id().equals(actionName)) {
                // We passed all the entries for actionName.  Therefore,
                // there are no other entries for pending nodes.
                return false;
            }
            if (!key.scope().equals("all")) {
                // We found another entry in the pending map for the action name.
                return true;
            }
        }
    }

    private synchronized void provide(Action action) {
        List<Dependency> newProvided = new ArrayList<>();
        for (String depId : action.provides()) {
            newProvided.add(new Dependency(depId, action.nodeName()));
            if (!otherNodesPending(depId)) {
                newProvided.add(new Dependency(depId, "all"));
            }
        }
        this.provided.addAll(newProvided);
        for (Dependency dep : newProvided) {
            Set<Action> waitingActions = waiters.remove(dep);
            if (waitingActions != null) {
                for (Iterator<Action> iter = waitingActions.iterator();
                        iter.hasNext(); ) {
                    Action waitingAction = iter.next();
                    iter.remove();
                    if (canSchedule(waitingAction)) {
                        schedule(waitingAction);
                    }
                }
            }
        }
    }

    private synchronized boolean canSchedule(Action action) {
        for (Requirement requirement : action.requirements()) {
            if (!this.provided.contains(requirement.dependency())) {
                return false;
            }
        }
        return true;
    }

    private synchronized void schedule(final Action action) {
        final SchedulerNode node = nodes.get(action.nodeName());
        if (node == null) {
            throw new RuntimeException("Unable to schedule " + action.toString() +
                " because there is no node " + action.nodeName() + " in the cluster.");
        }
        node.executorService.submit(new Runnable() {
            @Override
            public void run() {
                String prevNodeDescription;
                String nodeName = null;
                try {
                    synchronized (ActionScheduler.this) {
                        prevNodeDescription = node.description;
                        node.description = action.toString();
                        nodeName = node.nodeName;
                    }
                    SoakNode soakNode = cluster.nodes().get(nodeName);
                    if (actionFilter.matcher(action.toString()).matches()) {
                        SoakLog.printToAll(String.format("*** Running %s\n", action.toString()),
                            cluster.clusterLog(), soakNode.log());
                        action.call(cluster, soakNode);
                    } else {
                        cluster.clusterLog().printf("Skipping %s because it doesn't " +
                            "match pattern: %s\n", action.toString(), actionFilter);
                    }
                    boolean done = false;
                    synchronized (ActionScheduler.this) {
                        node.description = prevNodeDescription;
                        ActionScheduler.this.provide(action);
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
                        new RuntimeException(action.toString() + " failed", t);
                    shutdownFuture.completeExceptionally(e);
                    node.executorService.shutdownNow();
                    throw e;
                }
            }
        });
    }

    @Override
    public void close() {
        for (SchedulerNode node : nodes.values()) {
            node.executorService.shutdownNow();
        }
        for (SchedulerNode node : nodes.values()) {
            SoakUtil.awaitTerminationUninterruptibly(node.executorService);
        }
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
        }
        for (SchedulerNode node : nodes.values()) {
            node.executorService.shutdown();
        }
    }
}
