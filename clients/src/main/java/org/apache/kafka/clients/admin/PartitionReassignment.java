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

package org.apache.kafka.clients.admin;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes a partition reassignment.
 */
@InterfaceStability.Evolving
public class PartitionReassignment {
    /**
     * The topic name.
     */
    private final String topic;

    /**
     * The partition index.
     */
    private final int partition;

    /**
     * The set of replicas we want to store this partition on.
     * For backwards compatibility, this can also be referred to as simply "replicas".
     * If this is Optional.empty, any ongoing reassignment will be cancelled.
     */
    private final Optional<List<Integer>> targetReplicas;

    /**
     * The current set of replicas.  This is set by #{AdminClient#listPartitionReassignments}.
     */
    private final Optional<List<Integer>> currentReplicas;

    /**
     * The set of log directories we want to store this partition on.  This is used by the
     * kafka-reassign-partitions.sh tool, but is ignored by AdminClient.
     */
    private final Optional<List<String>> logDirs;

    public PartitionReassignment(String topic,
                                 int partition,
                                 Optional<List<Integer>> targetReplicas) {
        this(topic, partition, targetReplicas, Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public PartitionReassignment(@JsonProperty("topic") String topic,
            @JsonProperty("partition") int partition,
            @JsonAlias("replicas") @JsonProperty("targetReplicas") Optional<List<Integer>> targetReplicas,
            @JsonProperty("currentReplicas") Optional<List<Integer>> currentReplicas,
            @JsonProperty("logDirs") Optional<List<String>> logDirs) {
        this.topic = (topic == null) ? "" : topic;
        this.partition = partition;
        this.targetReplicas = Utils.createImmutableListOptionWithoutNulls(
                targetReplicas, Node.noNode().id());
        this.currentReplicas = Utils.createImmutableListOptionWithoutNulls(
                currentReplicas, Node.noNode().id());
        this.logDirs = Utils.createImmutableListOptionWithoutNulls(logDirs, "");
    }

    @JsonProperty
    public String topic() {
        return topic;
    }

    @JsonProperty
    public int partition() {
        return partition;
    }

    @JsonProperty
    public Optional<List<Integer>> targetReplicas() {
        return targetReplicas;
    }

    @JsonProperty
    public Optional<List<Integer>> currentReplicas() {
        return currentReplicas;
    }

    @JsonProperty
    public Optional<List<String>> logDirs() {
        return logDirs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, targetReplicas, currentReplicas, logDirs);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionReassignment)) {
            return false;
        }
        PartitionReassignment other = (PartitionReassignment) o;
        return topic.equals(other.topic) &&
                partition == other.partition &&
                targetReplicas.equals(other.targetReplicas) &&
                currentReplicas.equals(other.currentReplicas) &&
                logDirs.equals(other.logDirs);
    }
}
