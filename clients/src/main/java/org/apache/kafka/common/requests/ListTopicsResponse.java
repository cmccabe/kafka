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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ListTopicsResponse extends AbstractResponse {
    public static class Topic {
        private final String name;
        private final boolean internal;

        public Topic(String name, boolean internal) {
            this.name = name;
            this.internal = internal;
        }

        public String name() {
            return name;
        }

        public boolean internal() {
            return internal;
        }
    }

    private final static String ERROR_CODE = "error_code";

    private final static String TOPICS = "topics";

    private final static String NAME = "name";

    private final static String INTERNAL = "internal";

    private final Errors error;

    private final List<Topic> topics;

    public ListTopicsResponse(Errors error, List<Topic> topics) {
        this.error = error;
        this.topics = topics;
    }

    public ListTopicsResponse(Struct struct) {
        this.error = Errors.forCode(struct.getShort(ERROR_CODE));
        Object[] topicStructs = (Object[]) struct.get(TOPICS);
        List<Topic> topics = new ArrayList<>();
        for (Object topicStruct : topicStructs) {
            Struct topic = (Struct) topicStruct;
            String name = topic.getString(NAME);
            Boolean internal = topic.getBoolean(INTERNAL);
            topics.add(new Topic(name, internal));
        }
        this.topics = topics;
    }

    public Errors error() {
        return error;
    }

    public List<Topic> topics() {
        return topics;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct response = new Struct(ApiKeys.LIST_TOPICS.responseSchema(version));
        response.set(ERROR_CODE, error.code());
        List<Struct> topicList = new ArrayList<>(topics.size());
        for (Topic topic : topics) {
            Struct topicListing = new Struct(Protocol.TOPIC_LISTING);
            topicListing.set(NAME, topic.name());
            topicListing.set(INTERNAL, topic.internal());
            topicList.add(topicListing) ;
        }
        response.set(TOPICS, topicList.toArray());
        return response;
    }

    public static ListTopicsResponse parse(ByteBuffer buffer, short version) {
        return new ListTopicsResponse(ApiKeys.LIST_TOPICS.parseResponse(version, buffer));
    }
}
