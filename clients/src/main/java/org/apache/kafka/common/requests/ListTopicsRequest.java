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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ListTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListTopicsRequest> {
        private boolean listInternal;

        public Builder(boolean listInternal) {
            super(ApiKeys.LIST_TOPICS);
            this.listInternal = listInternal;
        }

        @Override
        public ListTopicsRequest build(short version) {
            return new ListTopicsRequest(listInternal, version);
        }

        @Override
        public String toString() {
            return "(type=ListTopicsRequest, listInternal=" + listInternal + ")";
        }
    }

    private final static String LIST_INTERNAL = "list_internal";

    private final boolean listInternal;

    private ListTopicsRequest(boolean listInternal, short version) {
        super(version);
        this.listInternal = listInternal;
    }

    ListTopicsRequest(Struct struct, short version) {
        super(version);
        this.listInternal = struct.getBoolean(LIST_INTERNAL);
    }

    public boolean listInternal() {
        return listInternal;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        short versionId = version();
        switch (versionId) {
            case 0:
                return new ListTopicsResponse(error, Collections.<ListTopicsResponse.Topic>emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LIST_TOPICS.latestVersion()));
        }
    }

    public static ListTopicsRequest parse(ByteBuffer buffer, short version) {
        return new ListTopicsRequest(ApiKeys.LIST_TOPICS.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.LIST_TOPICS.requestSchema(version()));
        struct.set(LIST_INTERNAL, listInternal);
        return struct;
    }
}
