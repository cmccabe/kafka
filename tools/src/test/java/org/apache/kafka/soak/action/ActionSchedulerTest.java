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

import org.apache.kafka.soak.cluster.MiniSoakCluster;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.cluster.SoakNodeSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ActionSchedulerTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateDestroy() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
            }
        }
    }

    @Test
    public void testRunActions() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node2", new SoakNodeSpec.Builder().build());
        final CyclicBarrier barrier = new CyclicBarrier(3);
        final AtomicInteger numRun = new AtomicInteger(0);
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            for (final String nodeName : miniCluster.cluster().nodes().keySet()) {
                schedulerBuilder.addAction(new Action(
                    new ActionId("testAction", nodeName),
                        new ActionId[0],
                        new ActionId[0]) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        numRun.incrementAndGet();
                        barrier.await();
                    }
                });
            }
            schedulerBuilder.addRoot("testAction");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
            }
        }
        assertEquals(3, numRun.get());
    }

    @Test
    public void testManyActionDependencies() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node2", new SoakNodeSpec.Builder().build());
        final AtomicInteger count = new AtomicInteger(0);
        final int numActions = 1000;
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            for (int actionIndex = 0; actionIndex < numActions; actionIndex++) {
                final int curActionIndex = actionIndex;
                String nodeName = String.format("node%d", actionIndex % 3);
                ActionId[] comesAfter = new ActionId[0];
                if (actionIndex != 0) {
                    comesAfter = new ActionId[] {
                        new ActionId(String.format("testAction%d", actionIndex - 1), ActionId.SCOPE_ALL)
                    };
                }
                ActionId[] implies = new ActionId[0];
                if (actionIndex != numActions - 1) {
                    implies = new ActionId[] {
                        new ActionId(String.format("testAction%d", actionIndex + 1), ActionId.SCOPE_ALL)
                    };
                }
                schedulerBuilder.addAction(new Action(
                        new ActionId(String.format("testAction%d", actionIndex), nodeName),
                            comesAfter, implies) {
                    @Override
                    public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                        int curCount = count.getAndIncrement();
                        if (curActionIndex != curCount) {
                            throw new RuntimeException("Expected count to be " +
                                curActionIndex + ", but it was " + curCount);
                        }
                    }
                });
                schedulerBuilder.addRoot("testAction0");
            }
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
            }
        }
        assertEquals(numActions, count.get());
    }

    private static final class ActionsPerNodeTracker {
        private final Map<String, Integer> map = new HashMap<>();

        synchronized void increment(String nodeName) {
            Integer cur = map.get(nodeName);
            if (cur == null) {
                cur = Integer.valueOf(0);
            }
            map.put(nodeName, cur + 1);
        }

        synchronized int actions(String nodeName) {
            return map.containsKey(nodeName) ? map.get(nodeName) : 0;
        }
    }

    @Test
    public void testSeveralActionDependencies() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node2", new SoakNodeSpec.Builder().build());
        final ActionsPerNodeTracker tracker = new ActionsPerNodeTracker();
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node0"),
                    new ActionId[0],
                    new ActionId[] {
                        new ActionId("bar", ActionId.SCOPE_ALL)
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    tracker.increment(node.nodeName());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node1"),
                    new ActionId[0],
                    new ActionId[] {
                        new ActionId("bar", ActionId.SCOPE_ALL)
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    tracker.increment(node.nodeName());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", "node2"),
                    new ActionId[]{
                        new ActionId("foo", "node0"),
                        new ActionId("foo", "node1")
                    },
                    new ActionId[] {
                        new ActionId("baz", ActionId.SCOPE_ALL)
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    tracker.increment(node.nodeName());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", "node0"),
                    new ActionId[]{
                        new ActionId("foo", "node0"),
                        new ActionId("foo", "node1"),
                        new ActionId("bar", "node2")
                    },
                    new ActionId[]{
                        new ActionId("baz", "node1")
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    tracker.increment(node.nodeName());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("baz", "node1"),
                    new ActionId[] {
                        new ActionId("bar", "node0")
                    },
                    new ActionId[] {}) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    tracker.increment(node.nodeName());
                }
            });
            schedulerBuilder.addRoot("foo:all");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
            }
            assertEquals(2, tracker.actions("node0"));
            assertEquals(2, tracker.actions("node1"));
            assertEquals(1, tracker.actions("node2"));
        }
    }

    @Test
    public void testMissingDependency() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        final CountDownLatch mainThreadLatch = new CountDownLatch(1);
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node0"),
                    new ActionId[]{
                        new ActionId("alpha", ActionId.SCOPE_ALL)
                    },
                    new ActionId[] {
                        new ActionId("bar", ActionId.SCOPE_ALL),
                        new ActionId("baz", "node0")
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    mainThreadLatch.countDown();
                }
            });
            schedulerBuilder.addRoot("foo:node0");
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
                mainThreadLatch.await();
            }
        }
    }

    @Test
    public void testAllDependency() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node2", new SoakNodeSpec.Builder().build());
        final CountDownLatch mainThreadLatch = new CountDownLatch(1);
        final CountDownLatch node1fooLatch = new CountDownLatch(1);
        final AtomicBoolean node2Done = new AtomicBoolean(false);
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node0"),
                    new ActionId[] {},
                    new ActionId[] {
                        new ActionId("bar", "node2")
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    mainThreadLatch.countDown();
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node1"),
                    new ActionId[] {},
                    new ActionId[]{
                        new ActionId("bar", "node2")
                    }) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    node1fooLatch.await();
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", "node2"),
                    new ActionId[] {
                        new ActionId("foo", "node1")
                    },
                    new ActionId[] {}) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    node2Done.set(true);
                }
            });
            schedulerBuilder.addRoot("foo:node0");
            schedulerBuilder.addRoot("foo:node1");

            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                assertEquals(false, node2Done.get());
                mainThreadLatch.await();
                assertEquals(false, node2Done.get());
                node1fooLatch.countDown();
                scheduler.await(100000000);
                assertEquals(true, node2Done.get());
            }
        }
    }

    @Test
    public void testActionFilter() throws Throwable {
        MiniSoakCluster.Builder clusterBuilder = new MiniSoakCluster.Builder();
        clusterBuilder.addNode("node0", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node1", new SoakNodeSpec.Builder().build());
        clusterBuilder.addNode("node2", new SoakNodeSpec.Builder().build());

        final ConcurrentSkipListSet<String> done = new ConcurrentSkipListSet<>();
        try (MiniSoakCluster miniCluster = clusterBuilder.build()) {
            ActionScheduler.Builder schedulerBuilder =
                new ActionScheduler.Builder(miniCluster.cluster());
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node0"),
                    new ActionId[] {},
                    new ActionId[] {}) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    done.add(this.toString());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("foo", "node1"),
                    new ActionId[] {},
                    new ActionId[] {}) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    done.add(this.toString());
                }
            });
            schedulerBuilder.addAction(new Action(
                new ActionId("bar", "node2"),
                    new ActionId[] {},
                    new ActionId[] {}) {
                @Override
                public void call(SoakCluster cluster, SoakNode node) throws Throwable {
                    done.add(this.toString());
                }
            });
            schedulerBuilder.addRoot("foo");
            schedulerBuilder.addRoot("bar");
            schedulerBuilder.actionFilter(Pattern.compile("(foo:node0|bar:node2)"));
            try (ActionScheduler scheduler = schedulerBuilder.build()) {
                scheduler.await(100000000);
            }
            assertTrue(done.contains("foo:node0"));
            assertFalse(done.contains("foo:node1"));
            assertTrue(done.contains("bar:node2"));
        }
    }
};
