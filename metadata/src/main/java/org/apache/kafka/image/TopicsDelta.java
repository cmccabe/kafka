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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents changes to the topics in the metadata image.
 */
public final class TopicsDelta {
    private final TopicsImage image;
    private final Map<Uuid, TopicDelta> changedTopics = new HashMap<>();

    public TopicsDelta(TopicsImage image) {
        this.image = image;
    }

    public void replay(TopicRecord record) {
        changedTopics.put(record.topicId(), new TopicDelta(
            new TopicImage(record.name(), record.topicId(), Collections.emptyMap())));
    }

    TopicDelta getOrCreateTopicDelta(Uuid id) {
        TopicDelta topicDelta = changedTopics.get(id);
        if (topicDelta == null) {
            topicDelta = new TopicDelta(image.getTopic(id));
            changedTopics.put(id, topicDelta);
        }
        return topicDelta;
    }

    public void replay(PartitionRecord record) {
        TopicDelta topicDelta = getOrCreateTopicDelta(record.topicId());
        topicDelta.replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        TopicDelta topicDelta = getOrCreateTopicDelta(record.topicId());
        topicDelta.replay(record);
    }

    public void replay(RemoveTopicRecord record) {
        TopicDelta topicDelta = getOrCreateTopicDelta(record.topicId());
        topicDelta.replay(record);
    }

    public TopicsImage apply() {
        Map<Uuid, TopicImage> newTopicsById = new HashMap<>(image.topicsById().size());
        Map<String, TopicImage> newTopicsByName = new HashMap<>(image.topicsByName().size());
        for (Entry<Uuid, TopicImage> entry : image.topicsById().entrySet()) {
            Uuid id = entry.getKey();
            TopicImage prevTopicImage = entry.getValue();
            TopicDelta delta = changedTopics.remove(id);
            if (delta == null) {
                newTopicsById.put(id, prevTopicImage);
                newTopicsByName.put(prevTopicImage.name(), prevTopicImage);
            } else if (!delta.isDeleted()) {
                TopicImage newTopicImage = delta.apply();
                newTopicsById.put(id, newTopicImage);
                newTopicsByName.put(delta.name(), newTopicImage);
            }
        }
        for (Entry<Uuid, TopicDelta> entry : changedTopics.entrySet()) {
            if (!newTopicsById.containsKey(entry.getKey())) {
                TopicImage newTopicImage = entry.getValue().apply();
                newTopicsById.put(newTopicImage.id(), newTopicImage);
                newTopicsByName.put(newTopicImage.name(), newTopicImage);
            }
        }
        return new TopicsImage(newTopicsById, newTopicsByName);
    }
}
