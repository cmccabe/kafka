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

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.controller.ReplicaAssignment;
import kafka.utils.CoreUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataState;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;
import scala.jdk.javaapi.CollectionConverters;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public final class ControllerUtils {
    public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    /**
     * A function which logs an exception at WARN level.
     */
    public static class WarnOnFailure implements Function<Throwable, Void> {
        private final String what;
        private final Logger log;

        public WarnOnFailure(String what, Logger log) {
            this.what = what;
            this.log = log;
        }

        @Override
        public Void apply(Throwable throwable) {
            log.warn("{} failed", what, throwable);
            return null;
        }
    }

    /**
     * Wait for a future to complete.  Log an error message if it fails.
     *
     * @param log       The logger object to use.
     * @param future    The future to wait for.
     */
    public static void await(Logger log, CompletableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            log.warn("Unexpected interruption.", e);
        } catch (ExecutionException e) {
            log.warn("Unexpected error waiting for future.", e);
        }
    }

    private final static DecimalFormat NANOS_TO_FRACTIONAL_MILLIS_DF =
        new DecimalFormat("#.####");

    public static String nanosToFractionalMillis(long ns) {
        float ms = ns;
        ms /= 1000000;
        return NANOS_TO_FRACTIONAL_MILLIS_DF.format(ms);
    }

    /**
     * Convert a kafka.cluster.Broker object into a MetadataState.Broker object.
     *
     * @param broker    The broker object to translate.
     * @return          The translated object.
     */
    public static MetadataState.Broker brokerToBrokerState(Broker broker) {
        MetadataState.Broker newBroker = new MetadataState.Broker();
        newBroker.setRack(OptionConverters.<String>
            toJava(broker.rack()).orElse(null));
        newBroker.setBrokerId(broker.id());
        List<MetadataState.BrokerEndpoint> newEndpoints = new ArrayList<>();
        for (EndPoint endPoint : CollectionConverters.asJava(broker.endPoints())) {
            MetadataState.BrokerEndpoint newEndpoint =
                new MetadataState.BrokerEndpoint();
            newEndpoint.setHost(endPoint.host());
            newEndpoint.setPort((short) endPoint.port());
            newEndpoint.setSecurityProtocol(endPoint.securityProtocol().id);
            newEndpoints.add(newEndpoint);
        }
        newBroker.setEndPoints(newEndpoints);
        return newBroker;
    }

    /**
     * Convert a replica assignment map into a TopicCollection.
     *
     * @param map       The input map.
     * @return          The topic collection.  Note that this will not contain ISR
     *                  information.
     */
    public static MetadataState.TopicCollection replicaAssignmentsToTopicStates(
            Map<TopicPartition, ReplicaAssignment> map) {
        MetadataState.TopicCollection newTopics =
            new MetadataState.TopicCollection();
        for (Map.Entry<TopicPartition, ReplicaAssignment> entry : map.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            MetadataState.Topic topic = newTopics.find(topicPartition.topic());
            if (topic == null) {
                topic = new MetadataState.Topic().setName(topicPartition.topic());
                newTopics.add(topic);
            }
            MetadataState.Partition partition = new MetadataState.Partition();
            partition.setId(topicPartition.partition());
            ReplicaAssignment replicaAssignment = entry.getValue();
            partition.setReplicas(CoreUtils.asJava(replicaAssignment.replicas()));
            partition.setAddingReplicas(
                CoreUtils.asJava(replicaAssignment.addingReplicas()));
            partition.setRemovingReplicas(
                CoreUtils.asJava(replicaAssignment.removingReplicas()));
            topic.partitions().add(partition);
        }
        return newTopics;
    }
}
