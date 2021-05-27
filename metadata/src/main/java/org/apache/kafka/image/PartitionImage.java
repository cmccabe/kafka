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


/**
 * Represents a partition in the metadata image.
 *
 * This class is thread-safe.
 */
public final class PartitionImage {
    private final int leaderId;
    private final int leaderEpoch;
    private final int[] replicas;
    private final int[] isr;
    private final int partitionEpoch;
    private final int[] removingReplicas;
    private final int[] addingReplicas;

    public PartitionImage(int leaderId,
                          int leaderEpoch,
                          int[] replicas,
                          int[] isr,
                          int partitionEpoch,
                          int[] removingReplicas,
                          int[] addingReplicas) {
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas = replicas;
        this.isr = isr;
        this.partitionEpoch = partitionEpoch;
        this.removingReplicas = removingReplicas;
        this.addingReplicas = addingReplicas;
    }

    public int leaderId() {
        return leaderId;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public int[] replicas() {
        return replicas;
    }

    public int[] isr() {
        return isr;
    }

    public int partitionEpoch() {
        return partitionEpoch;
    }

    public int[] removingReplicas() {
        return removingReplicas;
    }

    public int[] addingReplicas() {
        return addingReplicas;
    }
}
