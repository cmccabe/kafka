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
import org.apache.kafka.soak.cloud.Cloud;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.common.SoakUtil;
import org.apache.kafka.soak.role.Role;
import org.apache.kafka.soak.tool.SoakEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The SoakCluster.
 */
public final class SoakCluster implements AutoCloseable {
    private String soakId;
    private final String rootPath;
    private final String logPath;
    private final String kafkaPath;
    private final List<String> provided;
    private final TreeMap<String, String> defaults;
    private final Cloud cloud;
    private final SoakLog clusterLog;
    private final Map<String, SoakNode> nodes;

    public SoakCluster(SoakClusterSpec spec, SoakEnvironment env, Cloud cloud) throws IOException {
        this(spec.soakId(), env.rootPath(), env.kafkaPath(), spec.provided(), spec.defaults(),
            spec.nodes(), cloud, env.clusterLog());
    }

    public SoakCluster(String soakId,
                       String rootPath,
                       String kafkaPath,
                       List<String> provided,
                       Map<String, String> defaults,
                       Map<String, SoakNodeSpec> nodeSpecs,
                       Cloud cloud,
                       SoakLog clusterLog) throws IOException {
        this.soakId = soakId;
        this.rootPath = rootPath;
        this.logPath = Paths.get(rootPath, soakId).toString();
        Files.createDirectories(Paths.get(this.logPath));
        this.kafkaPath = kafkaPath;
        this.provided = provided;
        this.defaults = new TreeMap<>(defaults);
        this.cloud = cloud;
        this.clusterLog = clusterLog;
        TreeMap<String, SoakNode> nodes = new TreeMap<>();
        int nodeIndex = 0;
        for (Map.Entry<String, SoakNodeSpec> entry : nodeSpecs.entrySet()) {
            String nodeName = entry.getKey();
            SoakNodeSpec nodeSpec = entry.getValue();
            SoakLog soakLog = SoakLog.fromFile(logPath, nodeName);
            SoakNode node = new SoakNode(nodeIndex, nodeName, soakLog,
                    SoakUtil.mergeConfig(nodeSpec.conf(), defaults), nodeSpec);
            nodes.put(nodeName, node);
            nodeIndex++;
        }
        this.nodes = nodes;
    }

    public synchronized String soakId() {
        return soakId;
    }

    public synchronized void soakId(String soakId) {
        this.soakId = soakId;
    }

    public SoakLog clusterLog() {
        return clusterLog;
    }

    public Map<String, SoakNode> nodes() {
        return nodes;
    }

    public Collection<SoakNode> nodes(String... names) {
        List<SoakNode> foundNodes = new ArrayList<>();
        for (String name : names) {
            foundNodes.add(nodes.get(name));
        }
        return foundNodes;
    }

    public String rootPath() {
        return rootPath;
    }

    public String logPath() {
        return logPath;
    }

    public String kafkaPath() {
        return kafkaPath;
    }

    /**
     * Find nodes with a given role.
     *
     * @param roleClass     The role class.
     *
     * @return              A map from monontonically increasing integers to the names of
     *                      nodes.  The map will contain only nodes with the given role.
     */
    public TreeMap<Integer, String> nodesWithRole(Class<? extends Role> roleClass) {
        TreeMap<Integer, String> results = new TreeMap<>();
        int index = 0;
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            String nodeName = entry.getKey();
            SoakNode soakNode = entry.getValue();
            for (Role role : soakNode.spec().roles()) {
                if (roleClass.isInstance(role)) {
                    results.put(index, nodeName);
                    break;
                }
            }
            index++;
        }
        return results;
    }

    public Collection<String> getSoakNodesByNamesOrIndices(List<String> args) {
        if (args.contains("all")) {
            if (args.size() > 1) {
                throw new RuntimeException("Can't specify both 'all' and other node name(s).");
            }
            return Collections.unmodifiableSet(nodes.keySet());
        }
        TreeSet<String> nodesNames = new TreeSet<>();
        for (String arg : args) {
            nodesNames.add(getSoakNodeByNameOrIndex(arg));
        }
        return nodesNames;
    }

    public String getSoakNodeByNameOrIndex(String arg) {
        if (nodes.get(arg) != null) {
            // The argument was the name of a node.
            return arg;
        }
        // Try to parse the argument as a node number.
        int nodeIndex = -1;
        try {
            nodeIndex = Integer.parseInt(arg);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to find a node named " + arg);
        }
        int i = 0;
        for (Iterator<String> iter = nodes.keySet().iterator(); iter.hasNext(); ) {
            String entry = iter.next();
            if (i >= nodeIndex) {
                return entry;
            }
            i++;
        }
        throw new RuntimeException("Unable to find a node with index " +
            nodeIndex + "; we have only " + nodes.size() + " node(s).");
    }

    @Override
    public void close() {
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            Utils.closeQuietly(entry.getValue(), "cluster soakLogs");
        }
    }

    public List<String> provided() {
        return provided;
    }

    public Cloud cloud() {
        return cloud;
    }

    public synchronized SoakClusterSpec toSpec() {
        TreeMap<String, SoakNodeSpec> nodeSpecs = new TreeMap<>();
        for (Map.Entry<String, SoakNode> entry : nodes.entrySet()) {
            nodeSpecs.put(entry.getKey(), entry.getValue().spec());
        }
        return new SoakClusterSpec(soakId, nodeSpecs, defaults, provided);
    }
}
