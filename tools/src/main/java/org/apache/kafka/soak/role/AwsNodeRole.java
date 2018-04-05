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

package org.apache.kafka.soak.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.soak.action.Action;
import org.apache.kafka.soak.action.AwsDestroyAction;
import org.apache.kafka.soak.action.AwsInitAction;

import java.util.ArrayList;
import java.util.Collection;

public class AwsNodeRole implements Role {
    private final String imageId;
    private final String instanceType;
    private final String sshIdentityFile;
    private final String sshUser;

    @JsonCreator
    public AwsNodeRole(@JsonProperty("imageId") String imageId,
                       @JsonProperty("instanceType") String instanceType,
                       @JsonProperty("sshIdentityFile") String sshIdentityFile,
                       @JsonProperty("sshUser") String sshUser) {
        this.imageId = imageId;
        this.instanceType = instanceType;
        this.sshIdentityFile = sshIdentityFile;
        this.sshUser = sshUser;
    }

    public String imageId() {
        return imageId;
    }

    public String instanceType() {
        return instanceType;
    }

    public String sshIdentityFile() {
        return sshIdentityFile;
    }

    public String sshUser() {
        return sshUser;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new AwsInitAction(nodeName, this));
        actions.add(new AwsDestroyAction(nodeName));
        return actions;
    }
};
