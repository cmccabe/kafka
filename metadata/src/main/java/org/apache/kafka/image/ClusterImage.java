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

import java.util.Map;


/**
 * Represents the cluster in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ClusterImage {
    private final Map<Integer, NodeImage> brokers;

    public ClusterImage(Map<Integer, NodeImage> brokers) {
        this.brokers = brokers;
    }

    Map<Integer, NodeImage> brokers() {
        return brokers;
    }

    public NodeImage broker(int nodeId) {
        return brokers.get(nodeId);
    }
}
