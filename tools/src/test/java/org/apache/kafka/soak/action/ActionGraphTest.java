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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class ActionGraphTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private static class TestAction extends Action {
        public TestAction(ActionId id, ActionId[] comesAfter, ActionId[] implies) {
            super(id, comesAfter, implies);
        }
    }

    private ActionGraph.Builder createSampleBuilder() {
        return new ActionGraph.Builder().
            nodeNames(new HashSet<>(Arrays.asList(new String[] {"node0", "node1", "node2"}))).
            addAction(new TestAction(
                new ActionId("foo", "node0"),
                new ActionId[] {},
                new ActionId[] {new ActionId("bar", ActionId.SCOPE_ALL)}
            )).
            addAction(new TestAction(
                new ActionId("foo", "node1"),
                new ActionId[] {},
                new ActionId[] {new ActionId("bar", ActionId.SCOPE_ALL)}
            )).
            addAction(new TestAction(
                new ActionId("foo", "node2"),
                new ActionId[] {},
                new ActionId[] {new ActionId("bar", ActionId.SCOPE_ALL)}
            )).
            addAction(new TestAction(
                new ActionId("bar", "node0"),
                new ActionId[] {new ActionId("foo", ActionId.SCOPE_ALL)},
                new ActionId[] {new ActionId("baz", ActionId.SCOPE_ALL)}
            ));
    }

    @Test
    public void testMaterialize() throws Throwable {
        ActionGraph.Builder builder = createSampleBuilder();
        // Test that we can't materialize baz, because it doesn't exist.
        HashSet<ActionId> all = new HashSet<>();
        builder.materialize(new ActionId("baz", "node0"), all, all);
        assertEquals(0, all.size());
        builder.materialize(new ActionId("baz", "node1"), all, all);
        assertEquals(0, all.size());
        builder.materialize(new ActionId("baz", ActionId.SCOPE_ALL), all, all);
        assertEquals(0, all.size());

        // Materializing something concrete just gives us that thing.
        builder.materialize(new ActionId("foo", "node0"), all, all);
        assertEquals(Collections.singleton(new ActionId("foo", "node0")), all);
        all.clear();

        // Materializing an "all" target gives us everything from every node.
        HashSet<ActionId> expected = new HashSet<>();
        expected.add(new ActionId("foo", "node0"));
        expected.add(new ActionId("foo", "node1"));
        expected.add(new ActionId("foo", "node2"));
        builder.materialize(new ActionId("foo", ActionId.SCOPE_ALL), all, all);
        assertEquals(expected, all);
        all.clear();

        expected.clear();
        expected.add(new ActionId("bar", "node0"));
        builder.materialize(new ActionId("bar", ActionId.SCOPE_ALL), all, all);
        assertEquals(expected, all);
        all.clear();
    }

    @Test
    public void testEmptyGraph() throws Throwable {
        ActionGraph graph = createSampleBuilder().build();
        assertEquals(0, graph.actionMap().size());
        assertEquals(0, graph.concreteRoots().size());
    }

    @Test
    public void testPartialGraph() throws Throwable {
        ActionGraph graph = createSampleBuilder().addRoot("bar").build();
        assertEquals(1, graph.actionMap().size());
        assertEquals(1, graph.concreteRoots().size());
        assertEquals(new ActionId("bar", "node0"),
            graph.concreteRoots().iterator().next());
    }
};
