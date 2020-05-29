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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import kafka.api.LeaderAndIsr;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TopicDeltaTest {
    private static final Logger log = LoggerFactory.getLogger(TopicDeltaTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testFromSingleTopicRemoval() {
        TopicDelta delta = TopicDelta.fromSingleTopicRemoval("foo");
        assertEquals(Collections.emptyList(), delta.addedTopics);
        assertEquals(Arrays.asList("foo"), delta.removedTopics);
        assertEquals(Collections.emptyMap(), delta.addedParts);
        assertEquals(Collections.emptyList(), delta.removedParts);
        assertEquals(Collections.emptyMap(), delta.replicaChanges);
        assertEquals(Collections.emptyMap(), delta.isrChanges);
    }

    private static MetadataState.TopicCollection createTopics() {
        MetadataState.TopicCollection topics = new MetadataState.TopicCollection();
        topics.getOrCreate("foo").setPartitions(
            new MetadataState.PartitionCollection(Arrays.asList(
                new MetadataState.Partition().setId(0).
                    setReplicas(Arrays.asList(0, 1, 2)),
                new MetadataState.Partition().setId(1).
                    setReplicas(Arrays.asList(1, 2, 3)),
                new MetadataState.Partition().setId(2).
                    setReplicas(Arrays.asList(3, 0, 1))
            ).iterator()));
        topics.getOrCreate("bar").setPartitions(
            new MetadataState.PartitionCollection(Arrays.asList(
                new MetadataState.Partition().setId(0).
                    setReplicas(Arrays.asList(3, 2, 1)),
                new MetadataState.Partition().setId(1).
                    setReplicas(Arrays.asList(2, 1, 0))
            ).iterator()));
        return topics;
    }

    @Test
    public void testFromUpdatedTopicReplicasWithNoDifferences() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.Topic updatedTopic = existingTopics.find("foo").duplicate();
        TopicDelta delta = TopicDelta.
            fromUpdatedTopicReplicas(existingTopics, updatedTopic);
        assertEquals(Collections.emptyList(), delta.addedTopics);
        assertEquals(Collections.emptyList(), delta.removedTopics);
        assertEquals(Collections.emptyMap(), delta.addedParts);
        assertEquals(Collections.emptyList(), delta.removedParts);
        assertEquals(Collections.emptyMap(), delta.replicaChanges);
        assertEquals(Collections.emptyMap(), delta.isrChanges);
    }

    @Test
    public void testFromUpdatedTopicWithRemovedPartition() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.Topic updatedTopic = existingTopics.find("foo").duplicate();
        updatedTopic.partitions().remove(new MetadataState.Partition().setId(2));
        TopicDelta delta = TopicDelta.
            fromUpdatedTopicReplicas(existingTopics, updatedTopic);
        assertEquals(Collections.emptyList(), delta.addedTopics);
        assertEquals(Collections.emptyList(), delta.removedTopics);
        assertEquals(Collections.emptyMap(), delta.addedParts);
        assertEquals(Collections.singletonList(new TopicPartition("foo", 2)),
            delta.removedParts);
        assertEquals(Collections.emptyMap(), delta.replicaChanges);
        assertEquals(Collections.emptyMap(), delta.isrChanges);
    }

    @Test
    public void testFromUpdatedTopicReplicas() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.TopicCollection updatedTopics = existingTopics.duplicate();
        updatedTopics.find("bar").partitions().find(0).setReplicas(Arrays.asList(2, 1));
        updatedTopics.add(new MetadataState.Topic().setName("baz"));
        updatedTopics.remove(new MetadataState.Topic().setName("foo"));
        TopicDelta delta = TopicDelta.
            fromUpdatedTopicReplicas(existingTopics, updatedTopics);
        assertEquals(Collections.singletonList("baz"),
            delta.addedTopics.stream().map(t -> t.name()).collect(Collectors.toList()));
        assertEquals(Collections.singletonList("foo"), delta.removedTopics);
        assertEquals(Collections.emptyMap(), delta.addedParts);
        assertEquals(Collections.emptyList(), delta.removedParts);
        assertEquals(Collections.singletonMap(new TopicPartition("bar", 0),
            new TopicDelta.ReplicaChange(Arrays.asList(2, 1), Collections.emptyList(),
                Collections.emptyList())), delta.replicaChanges);
        assertEquals(Collections.emptyMap(), delta.isrChanges);
        delta.apply(existingTopics);
        assertNotNull(existingTopics.find("baz"));
        assertEquals(null, existingTopics.find("foo"));
    }

    @Test
    public void testFromUpdatedTopics2() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.TopicCollection updatedTopics = existingTopics.duplicate();
        updatedTopics.find("bar").partitions().getOrCreate(2).
            setReplicas(Arrays.asList(1, 0, 3));
        updatedTopics.find("bar").partitions().find(0).
            setReplicas(Arrays.asList(3, 2, 1, 4));
        TopicDelta delta = TopicDelta.
            fromUpdatedTopicReplicas(existingTopics, updatedTopics);
        assertEquals(Collections.emptyList(), delta.addedTopics);
        assertEquals(Collections.emptyList(), delta.removedTopics);
        assertEquals(Collections.singleton(new TopicPartition("bar", 2)),
            delta.addedParts.keySet());
        assertEquals(Collections.emptyList(), delta.removedParts);
        assertEquals(Collections.singletonMap(new TopicPartition("bar", 0),
            new TopicDelta.ReplicaChange(Arrays.asList(3, 2, 1, 4), Collections.emptyList(),
                Collections.emptyList())), delta.replicaChanges);
        assertEquals(Collections.emptyMap(), delta.isrChanges);
        delta.apply(existingTopics);
        assertEquals(Arrays.asList(3, 2, 1, 4),
            existingTopics.find("bar").partitions().find(0).replicas());
        assertEquals(Arrays.asList(1, 0, 3),
            existingTopics.find("bar").partitions().find(2).replicas());
        TopicDelta delta2 = TopicDelta.
            fromUpdatedTopicReplicas(existingTopics, updatedTopics);
        assertEquals(Collections.emptyMap(), delta2.replicaChanges);
    }

    @Test
    public void testFromIsrUpdates() {
        MetadataState.TopicCollection existingTopics = createTopics();
        Map<TopicPartition, LeaderIsrAndControllerEpoch> updates = new HashMap<>();
        updates.put(new TopicPartition("foo", 0),
            new LeaderIsrAndControllerEpoch(new LeaderAndIsr(0, 100,
                CoreUtils.asScala(Arrays.asList(0, 1)), 456), 123));
        TopicDelta delta = TopicDelta.fromIsrUpdates(existingTopics, updates);
        assertEquals(Collections.emptyList(), delta.addedTopics);
        assertEquals(Collections.emptyList(), delta.removedTopics);
        assertEquals(Collections.emptyMap(), delta.addedParts);
        assertEquals(Collections.emptyList(), delta.removedParts);
        assertEquals(Collections.emptyMap(), delta.replicaChanges);
        assertEquals(Collections.singletonMap(new TopicPartition("foo", 0),
            new TopicDelta.IsrChange(0, 100, Arrays.asList(0, 1), 123)),
                delta.isrChanges);
        delta.apply(existingTopics);
        TopicDelta delta2 = TopicDelta.fromIsrUpdates(existingTopics, updates);
        assertEquals(Collections.emptyMap(), delta2.isrChanges);
    }
}
