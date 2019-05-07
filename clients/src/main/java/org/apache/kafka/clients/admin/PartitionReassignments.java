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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Describes some partition reassignments.
 */
@InterfaceStability.Evolving
public class PartitionReassignments {
    private final static int VALID_VERSION = 1;
    private final List<PartitionReassignment> partitions;

    public PartitionReassignments(List<PartitionReassignment> partitions) {
        this(VALID_VERSION, partitions);
    }

    @JsonCreator
    public PartitionReassignments(@JsonProperty("version") int version,
            @JsonProperty("partitions") List<PartitionReassignment> partitions) {
        if (version != VALID_VERSION) {
            throw new UnsupportedVersionException("Unsupported PartitionReassignments version " +
                version + ".  Supported version(s) are: " + VALID_VERSION);
        }
        this.partitions = (partitions == null) ? Collections.emptyList() :
            Collections.unmodifiableList(new ArrayList<>(partitions));
    }

    @JsonProperty
    public int version() {
        return VALID_VERSION;
    }

    @JsonProperty
    public List<PartitionReassignment> partitions() {
        return partitions;
    }
}
