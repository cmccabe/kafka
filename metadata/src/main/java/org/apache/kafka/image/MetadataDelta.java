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

import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;


/**
 * The broker metadata image.
 *
 * This class is thread-safe.
 */
public final class MetadataDelta {
    private final MetadataImage image;

    private ClusterDelta clusterDelta = null;

    private TopicsDelta topicsDelta = null;

    private ConfigurationsDelta configsDelta = null;

    public MetadataDelta(MetadataImage image) {
        this.image = image;
    }

    public void replay(RegisterBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(UnregisterBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(TopicRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(PartitionRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(FenceBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(UnfenceBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(RemoveTopicRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(ConfigRecord record) {
        if (configsDelta == null) configsDelta = new ConfigurationsDelta(image.configs());
        configsDelta.replay(record);
    }

    public MetadataImage apply() {
        ClusterImage newCluster;
        if (clusterDelta == null) {
            newCluster = image.cluster();
        } else {
            newCluster = clusterDelta.apply();
        }
        TopicsImage newTopics;
        if (topicsDelta == null) {
            newTopics = image.topics();
        } else {
            newTopics = topicsDelta.apply();
        }
        ConfigurationsImage newConfigs;
        if (configsDelta == null) {
            newConfigs = image.configs();
        } else {
            newConfigs = configsDelta.apply();
        }
        return new MetadataImage(newCluster, newTopics, newConfigs);
    }
}
