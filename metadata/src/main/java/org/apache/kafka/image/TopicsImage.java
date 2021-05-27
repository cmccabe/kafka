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

import java.util.Map;


/**
 * Represents the topics in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicsImage {
    private final Map<Uuid, TopicImage> topicsById;
    private final Map<String, TopicImage> topicsByName;

    public TopicsImage(Map<Uuid, TopicImage> topicsById,
                       Map<String, TopicImage> topicsByName) {
        this.topicsById = topicsById;
        this.topicsByName = topicsByName;
    }

    Map<Uuid, TopicImage> topicsById() {
        return topicsById;
    }

    Map<String, TopicImage> topicsByName() {
        return topicsByName;
    }

    public TopicImage getTopic(Uuid id) {
        return topicsById.get(id);
    }

    public TopicImage getTopic(String name) {
        return topicsByName.get(name);
    }
}
