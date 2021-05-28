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

import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents changes to a topic in the metadata image.
 */
public final class TopicDelta {
    private final TopicImage image;
    private final Map<Integer, PartitionImage> partitionChanges = new HashMap<>();
    private boolean deleted = false;

    public TopicDelta(TopicImage image) {
        this.image = image;
    }

    public String name() {
        return image.name();
    }

    public void replay(PartitionRecord record) {
        deleted = false;
        partitionChanges.put(record.partitionId(), new PartitionImage(record));
    }

    public void replay(PartitionChangeRecord record) {
        if (deleted) {
            throw new RuntimeException("Can't modify partition " + record.partitionId() +
                " on topic " + image.name() + " with id " + image.id() + ", since the " +
                "topic was deleted.");
        }
        PartitionImage partitionImage = partitionChanges.get(record.partitionId());
        if (partitionImage == null) {
            partitionImage = image.partitions().get(record.partitionId());
            if (partitionImage == null) {
                throw new RuntimeException("Unable to find partition " +
                    record.topicId() + ":" + record.partitionId());
            }
        }
        partitionChanges.put(record.partitionId(), new PartitionImage(partitionImage, record));
    }

    public void replay(RemoveTopicRecord record) {
        deleted = true;
        partitionChanges.clear();
    }

    public boolean isDeleted() {
        return deleted;
    }

    public TopicImage apply() {
        if (deleted) {
            return null;
        }
        Map<Integer, PartitionImage> newPartitions = new HashMap<>();
        for (Entry<Integer, PartitionImage> entry : image.partitions().entrySet()) {
            int partitionId = entry.getKey();
            PartitionImage changedPartition = partitionChanges.get(partitionId);
            if (changedPartition == null) {
                newPartitions.put(partitionId, entry.getValue());
            } else {
                newPartitions.put(partitionId, changedPartition);
            }
        }
        for (Entry<Integer, PartitionImage> entry : partitionChanges.entrySet()) {
            if (!newPartitions.containsKey(entry.getKey())) {
                newPartitions.put(entry.getKey(), entry.getValue());
            }
        }
        return new TopicImage(image.name(), image.id(), newPartitions);
    }
}
