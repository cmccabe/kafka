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

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ActionGraph {
    private static final Logger log = LoggerFactory.getLogger(ActionGraph.class);

    public static class Builder {
        private Set<String> nodeNames = null;
        private final HashMap<ActionId, Action> actions = new HashMap<>();
        private final HashSet<ActionId> roots = new HashSet<>();

        public Builder nodeNames(Set<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        public Builder addAction(Action action) {
            actions.put(action.id(), action);
            return this;
        }

        public Builder addActions(Collection<Action> actions) {
            for (Action action : actions) {
                addAction(action);
            }
            return this;
        }

        public Builder addRoot(String rootString) {
            return addRootId(ActionId.parse(rootString));
        }

        public Builder addRootId(ActionId rootId) {
            roots.add(rootId);
            return this;
        }

        public Builder addRoots(Collection<String> roots) {
            for (String root : roots) {
                addRoot(root);
            }
            return this;
        }

        int materialize(ActionId id, Collection<ActionId> prevIds, Collection<ActionId> output) {
            if (id.hasGlobalScope()) {
                int count = 0;
                for (String nodeName : nodeNames) {
                    count += materialize(new ActionId(id.type(), nodeName), prevIds, output);
                }
                return count;
            } else {
                if (!actions.containsKey(id)) {
                    return 0;
                } else if (prevIds.contains(id)) {
                    return 0;
                } else {
                    output.add(id);
                    return 1;
                }
            }
        }

        public ActionGraph build() {
            if (nodeNames == null) {
                throw new RuntimeException("You must set the node names.");
            }
            HashSet<ActionId> concreteRoots = new HashSet<>();
            for (ActionId id : roots) {
                if (materialize(id, concreteRoots, concreteRoots) == 0) {
                    throw new RuntimeException("Root " + id + " not found.");
                }
            }
            Deque<ActionId> toSearch = new ArrayDeque<>(concreteRoots);
            HashSet<ActionId> active = new HashSet<>();
            for (ActionId id = toSearch.pollFirst(); id != null; id = toSearch.pollFirst()) {
                if (materialize(id, active, active) > 0) {
                    Action action = actions.get(id);
                    for (ActionId nextId : action.implies()) {
                        materialize(nextId, active, toSearch);
                    }
                }
            }
            Map<ActionId, Set<ActionId>> comesBeforeMap = new HashMap<>();
            for (ActionId id : active) {
                Action action = actions.get(id);
                Set<ActionId> concreteComesAfter = new HashSet<>();
                for (ActionId comesAfterId : action.comesAfter()) {
                    materialize(comesAfterId, concreteComesAfter, concreteComesAfter);
                }
                log.info("WATERMLEON: actions.comesAfter() = {}, concreteComesAfter = {}",
                    Utils.join(action.comesAfter(), ", "),
                    Utils.join(concreteComesAfter, ", "));
                for (ActionId comesAfterId : concreteComesAfter) {
                    Set<ActionId> comesBefore = comesBeforeMap.get(comesAfterId);
                    if (comesBefore == null) {
                        comesBefore = new HashSet<>();
                        comesBeforeMap.put(comesAfterId, comesBefore);
                    }
                    comesBefore.add(id);
                }
            }
            Map<ActionId, ActionData> actionMap = new HashMap<>();
            for (ActionId id : active) {
                Action action = actions.get(id);
                Set<ActionId> comesBefore = comesBeforeMap.get(id);
                if (comesBefore == null) {
                    comesBefore = new HashSet<>();
                }
                actionMap.put(id, new ActionData(action, comesBefore));
            }
            return new ActionGraph(concreteRoots, actionMap);
        }
    }

    public static class ActionData {
        private Action action;
        private Set<ActionId> comesBefore;

        ActionData(Action action, Set<ActionId> comesBefore) {
            this.action = action;
            this.comesBefore = Collections.unmodifiableSet(comesBefore);
        }

        public Action action() {
            return action;
        }

        public Set<ActionId> comesBefore() {
            return comesBefore;
        }
    }

    private final Set<ActionId> concreteRoots;

    private final Map<ActionId, ActionData> actionMap;

    public ActionGraph(Set<ActionId> concreteRoots, Map<ActionId, ActionData> actionMap) {
        this.concreteRoots = Collections.unmodifiableSet(concreteRoots);
        this.actionMap = Collections.unmodifiableMap(actionMap);
    }

    public Set<ActionId> concreteRoots() {
        return concreteRoots;
    }

    public Map<ActionId, ActionData> actionMap() {
        return actionMap;
    }
}
