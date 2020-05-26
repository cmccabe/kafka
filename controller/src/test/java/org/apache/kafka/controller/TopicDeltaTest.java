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
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.message.MetadataState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TopicDeltaTest {
    private static final Logger log = LoggerFactory.getLogger(TopicDeltaTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testFromSingleTopicRemoval() {
        TopicDelta topicDelta = TopicDelta.fromSingleTopicRemoval("foo");
        assertEquals(Collections.emptyList(), topicDelta.addedTopics);
        assertEquals(Arrays.asList("foo"), topicDelta.removedTopics);
        assertEquals(Collections.emptyMap(), topicDelta.addedParts);
        assertEquals(Collections.emptyList(), topicDelta.removedParts);
        assertEquals(Collections.emptyMap(), topicDelta.addedReplicas);
        assertEquals(Collections.emptyList(), topicDelta.removedReplicas);
    }

    private static MetadataState.TopicCollection createTopics() {
        MetadataState.TopicCollection topics = new MetadataState.TopicCollection();
        topics.add(new MetadataState.Topic().setName("foo").setPartitions(
            new MetadataState.PartitionCollection(Arrays.asList(
                new MetadataState.Partition().setId(0).setReplicas(
                    new MetadataState.ReplicaCollection(Arrays.asList(
                        new MetadataState.Replica().setId(0),
                        new MetadataState.Replica().setId(1),
                        new MetadataState.Replica().setId(2)).iterator())),
                new MetadataState.Partition().setId(1).setReplicas(
                    new MetadataState.ReplicaCollection(Arrays.asList(
                        new MetadataState.Replica().setId(1),
                        new MetadataState.Replica().setId(2),
                        new MetadataState.Replica().setId(3)).iterator())),
                new MetadataState.Partition().setId(2).setReplicas(
                    new MetadataState.ReplicaCollection(Arrays.asList(
                        new MetadataState.Replica().setId(3),
                        new MetadataState.Replica().setId(0),
                        new MetadataState.Replica().setId(1)).iterator()))
            ).iterator())));
        topics.add(new MetadataState.Topic().setName("bar").setPartitions(
            new MetadataState.PartitionCollection(Arrays.asList(
                new MetadataState.Partition().setId(0).setReplicas(
                    new MetadataState.ReplicaCollection(Arrays.asList(
                        new MetadataState.Replica().setId(3),
                        new MetadataState.Replica().setId(2),
                        new MetadataState.Replica().setId(1)).iterator())),
                new MetadataState.Partition().setId(1).setReplicas(
                    new MetadataState.ReplicaCollection(Arrays.asList(
                        new MetadataState.Replica().setId(2),
                        new MetadataState.Replica().setId(1),
                        new MetadataState.Replica().setId(0)).iterator()))
            ).iterator())));
        return topics;
    }

    @Test
    public void testFromUpdatedTopicWithNoDifferences() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.Topic updatedTopic = existingTopics.find("foo").duplicate();
        TopicDelta topicDelta = TopicDelta.fromUpdatedTopic(existingTopics, updatedTopic);
        assertEquals(Collections.emptyList(), topicDelta.addedTopics);
        assertEquals(Collections.emptyList(), topicDelta.removedTopics);
        assertEquals(Collections.emptyMap(), topicDelta.addedParts);
        assertEquals(Collections.emptyList(), topicDelta.removedParts);
        assertEquals(Collections.emptyMap(), topicDelta.addedReplicas);
        assertEquals(Collections.emptyList(), topicDelta.removedReplicas);
    }

    @Test
    public void testFromUpdatedTopicWithRemovedPartition() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.Topic updatedTopic = existingTopics.find("foo").duplicate();
        updatedTopic.partitions().remove(new MetadataState.Partition().setId(2));
        TopicDelta topicDelta = TopicDelta.fromUpdatedTopic(existingTopics, updatedTopic);
        assertEquals(Collections.emptyList(), topicDelta.addedTopics);
        assertEquals(Collections.emptyList(), topicDelta.removedTopics);
        assertEquals(Collections.emptyMap(), topicDelta.addedParts);
        assertEquals(Collections.singletonList(new TopicPartition("foo", 2)),
            topicDelta.removedParts);
        assertEquals(Collections.emptyMap(), topicDelta.addedReplicas);
        assertEquals(Collections.emptyList(), topicDelta.removedReplicas);
    }

    @Test
    public void testFromUpdatedTopics() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.TopicCollection updatedTopics = existingTopics.duplicate();
        updatedTopics.find("bar").partitions().find(0).replicas().
            remove(new MetadataState.Replica().setId(3));
        updatedTopics.add(new MetadataState.Topic().setName("baz"));
        updatedTopics.remove(new MetadataState.Topic().setName("foo"));
        TopicDelta topicDelta = TopicDelta.fromUpdatedTopics(existingTopics, updatedTopics);
        assertEquals(Collections.singletonList("baz"),
            topicDelta.addedTopics.stream().map(t -> t.name()).collect(Collectors.toList()));
        assertEquals(Collections.singletonList("foo"), topicDelta.removedTopics);
        assertEquals(Collections.emptyMap(), topicDelta.addedParts);
        assertEquals(Collections.emptyList(), topicDelta.removedParts);
        assertEquals(Collections.emptyMap(), topicDelta.addedReplicas);
        assertEquals(Collections.singletonList(new TopicPartitionReplica("bar", 0, 3)),
            topicDelta.removedReplicas);
    }

    @Test
    public void testFromUpdatedTopics2() {
        MetadataState.TopicCollection existingTopics = createTopics();
        MetadataState.TopicCollection updatedTopics = existingTopics.duplicate();
        updatedTopics.find("bar").partitions().add(
            new MetadataState.Partition().setId(2).setReplicas(
                new MetadataState.ReplicaCollection(Arrays.asList(
                    new MetadataState.Replica().setId(1),
                    new MetadataState.Replica().setId(0),
                    new MetadataState.Replica().setId(3)).iterator())));
        updatedTopics.find("bar").partitions().find(0).replicas().
            add(new MetadataState.Replica().setId(4));
        TopicDelta topicDelta = TopicDelta.fromUpdatedTopics(existingTopics, updatedTopics);
        assertEquals(Collections.emptyList(), topicDelta.addedTopics);
        assertEquals(Collections.emptyList(), topicDelta.removedTopics);
        assertEquals(Collections.singleton(new TopicPartition("bar", 2)),
            topicDelta.addedParts.keySet());
        assertEquals(Collections.emptyList(), topicDelta.removedParts);
        assertEquals(Collections.singleton(new TopicPartitionReplica("bar", 0, 4)),
            topicDelta.addedReplicas.keySet());
        assertEquals(Collections.emptyList(), topicDelta.removedReplicas);
    }
}
