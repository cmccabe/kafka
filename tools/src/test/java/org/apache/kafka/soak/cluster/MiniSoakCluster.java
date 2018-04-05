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

package org.apache.kafka.soak.cluster;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.action.ActionScheduler;
import org.apache.kafka.soak.cloud.MockCloud;
import org.apache.kafka.soak.cloud.MockRemoteCommand;
import org.apache.kafka.soak.common.NullOutputStream;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.tool.SoakEnvironment;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MiniSoakCluster implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MiniSoakCluster.class);

    public static class Builder {
        private final Map<String, SoakNodeSpec> nodeSpecs = new TreeMap<>();
        private Map<String, String> defaults = new TreeMap<>();
        private MockCloud cloud = new MockCloud();
        private String actionFilter = ActionScheduler.DEFAULT_ACTION_FILTER.toString();

        public Builder() {
        }

        public Builder addNode(String nodeName, SoakNodeSpec node) {
            nodeSpecs.put(nodeName, node);
            return this;
        }

        public Builder addNodeWithInstanceId(String nodeName, SoakNodeSpec node) throws Exception {
            String instanceId = cloud.runInstance(new TreeMap<String, String>());
            nodeSpecs.put(nodeName,
                new SoakNodeSpec.Builder(node).instanceId(instanceId).
                    publicDns(MockCloud.publicDns(instanceId)).
                    privateDns(MockCloud.privateDns(instanceId)).
                    build());
            return this;
        }

        public Builder defaults(Map<String, String> defaults) {
            this.defaults = defaults;
            return this;
        }

        public Builder actionFilter(String actionFilter) {
            this.actionFilter = actionFilter;
            return this;
        }

        public MiniSoakCluster build() throws IOException {
            SoakCluster soakCluster = null;
            File tempDirectory = TestUtils.tempDirectory();
            boolean success = false;
            try {
                Path outputPath =Paths.get(tempDirectory.getAbsolutePath(), "output");
                Files.createDirectories(outputPath);
                SoakEnvironment env = new SoakEnvironment(
                    Paths.get(tempDirectory.getAbsolutePath(), "testSpec.json").toString(),
                    "",
                    "",
                    360,
                    actionFilter,
                    Paths.get(tempDirectory.getAbsolutePath(), "kafka").toString(),
                    outputPath.toString());
                soakCluster = new SoakCluster(env,
                    cloud,
                    new SoakLog(SoakLog.CLUSTER, NullOutputStream.INSTANCE),
                    new SoakClusterSpec(nodeSpecs, defaults));
                success = true;
            } finally {
                if (!success)  {
                    Utils.delete(tempDirectory);
                }
            }
            return new MiniSoakCluster(soakCluster, tempDirectory, cloud);
        }
    }

    private final SoakCluster cluster;

    private final File tempDirectory;

    private final MockCloud cloud;

    private MiniSoakCluster(SoakCluster cluster, File tempDirectory, MockCloud cloud) throws IOException {
        this.cluster = cluster;
        this.tempDirectory = tempDirectory;
        this.cloud = cloud;
    }

    public SoakCluster cluster() {
        return cluster;
    }

    public MockCloud cloud() {
        return cloud;
    }

    public String[] rsyncToCommandLine(String nodeName, String local, String remote) {
        return MockRemoteCommand.rsyncToCommandLine(cluster.nodes().get(nodeName), local, remote);
    }

    public String[] rsyncFromCommandLine(String nodeName, String remote, String local) {
        return MockRemoteCommand.rsyncFromCommandLine(cluster.nodes().get(nodeName), remote, local);
    }

    public void close() {
        try {
            Utils.delete(tempDirectory);
        } catch (IOException e) {
            log.error("Failed to delete {}", tempDirectory.getAbsolutePath(), e);
        }
    }
}
