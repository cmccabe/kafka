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

package org.apache.kafka.soak.cloud;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MockCloud implements Cloud {
    private final Logger log = LoggerFactory.getLogger(MockCloud.class);

    private static final class MockCloudInstance {
        final String id;
        private Map<String, String> descriptions;

        MockCloudInstance(String id, Map<String, String> descriptions) {
            this.id = id;
            this.descriptions = Collections.unmodifiableMap(new TreeMap<>(descriptions));
        }

        @Override
        public String toString() {
            return "(id=" + id + ", descriptions=" +
                Utils.mkString(descriptions, "{", "}", "=", ",") + ")";
        }
    }

    private final class CommandResponseTracker {
        private final String nodeName;
        private final HashMap<List<String>, ArrayDeque<MockCommandResponse>> commandResponses;
        private int commandResponsesRemoved;

        CommandResponseTracker(String nodeName) {
            this.nodeName = nodeName;
            this.commandResponses = new HashMap<>();
            this.commandResponsesRemoved = 0;
        }

        public void add(MockCommandResponse response) {
            ArrayDeque<MockCommandResponse> queue = this.commandResponses.get(response.request());
            if (queue == null) {
                queue = new ArrayDeque<>();
                this.commandResponses.put(response.request(), queue);
            }
            log.trace("{}: CommandResponseTracker registered command line {}", nodeName,
                SoakRemoteCommand.joinCommandLineArgs(response.request()));
            queue.addLast(response);
        }

        public MockCommandResponse remove(List<String> commandLine) {
            ArrayDeque<MockCommandResponse> queue = this.commandResponses.get(commandLine);
            if (queue == null) {
                return null;
            }
            MockCommandResponse response = queue.removeFirst();
            if (queue.isEmpty()) {
                this.commandResponses.remove(commandLine);
            }
            if (response != null) {
                commandResponsesRemoved++;
            }
            return response;
        }

        public int commandResponsesRemoved() {
            return commandResponsesRemoved;
        }
    }

    private Map<String, MockCloudInstance> instances = new HashMap<>();

    private final HashMap<String, CommandResponseTracker> commandResponseTrackers = new HashMap<>();

    private int idCounter = 0;

    @Override
    public synchronized String runInstance(Map<String, String> params) throws Exception {
        String instanceId = nextId();
        TreeMap<String, String> descriptions = new TreeMap<>();
        descriptions.put(SoakConfig.PUBLIC_DNS, publicDns(instanceId));
        descriptions.put(SoakConfig.PRIVATE_DNS, privateDns(instanceId));
        MockCloudInstance instance = new MockCloudInstance(instanceId, descriptions);
        instances.put(instanceId, instance);
        log.trace("runInstance: created instance {}", instance);
        return instanceId;
    }

    private synchronized String nextId() {
        return String.format("inst%02d", idCounter++);
    }

    @Override
    public synchronized Map<String, String> describeInstance(String instanceId) throws Exception {
        MockCloudInstance instance = instances.get(instanceId);
        if (instance == null) {
            return null;
        }
        return Collections.unmodifiableMap(new TreeMap<>(instance.descriptions));
    }

    public static String publicDns(String instanceId) {
        return String.format("%s.public.example.com", instanceId);
    }

    public static String privateDns(String instanceId) {
        return String.format("%s.private.example.com", instanceId);
    }

    @Override
    public synchronized void terminateInstances(String... instanceIds) throws Exception {
        RuntimeException exception = null;
        List<String> terminated = new ArrayList<>();
        for (String instanceId : instanceIds) {
            if (instances.remove(instanceId) == null) {
                if (exception == null) {
                    exception = new RuntimeException("instance " + instanceId + " is not running.");
                }
            } else {
                terminated.add(instanceId);
            }
        }
        log.trace("terminated instance(s) {}", Utils.join(terminated, ", "));
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public RemoteCommand remoteCommand(SoakNode node) {
        return new MockRemoteCommand(this, node);
    }

    public synchronized void addCommandResponse(String nodeName, MockCommandResponse response) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            tracker = new CommandResponseTracker(nodeName);
            commandResponseTrackers.put(nodeName, tracker);
        }
        tracker.add(response);
    }

    public synchronized MockCommandResponse removeResponse(String nodeName, List<String> commandLine) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            log.trace("removeResponse({}, {}): no tracker found for {}",
                nodeName, SoakRemoteCommand.joinCommandLineArgs(commandLine), nodeName);
            return null;
        }
        MockCommandResponse response = tracker.remove(commandLine);
        return response;
    }

    public synchronized int commandResponsesRemoved(String nodeName) {
        CommandResponseTracker tracker = commandResponseTrackers.get(nodeName);
        if (tracker == null) {
            return 0;
        }
        return tracker.commandResponsesRemoved();
    }

    public synchronized int numInstances() {
        return instances.size();
    }

    @Override
    public void close() throws Exception {
        // Nothing to do here
    }
}
