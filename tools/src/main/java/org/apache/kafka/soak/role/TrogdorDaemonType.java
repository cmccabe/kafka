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

public class TrogdorDaemonType {
    private final String name;
    private final String providesPrefix;
    private final String root;
    private final String className;

    private TrogdorDaemonType(String name, String providesPrefix, String root, String className) {
        this.name = name;
        this.providesPrefix = providesPrefix;
        this.root = root;
        this.className = className;
    }

    public String name() {
        return name;
    }

    public String providesPrefix() {
        return providesPrefix;
    }

    public String root() {
        return root;
    }

    public String className() {
        return className;
    }

    public final String logName() {
        return name + ".log";
    }

    public final String start() {
        return providesPrefix + RoleDependencies.START_SUFFIX;
    }

    public final String status() {
        return providesPrefix + RoleDependencies.STATUS_SUFFIX;
    }

    public final String stop() {
        return providesPrefix + RoleDependencies.STOP_SUFFIX;
    }

    public String propertiesPath() {
        return String.format("%s%s%s", root, RolePaths.TROGDOR_CONF_SUFFIX,
            RolePaths.TROGDOR_PROPERTIES_SUFFIX);
    }

    public String log4jConfPath() {
        return String.format("%s%s%s", root, RolePaths.TROGDOR_CONF_SUFFIX,
            RolePaths.TROGDOR_LOG4J_SUFFIX);
    }

    public String logPath() {
        return String.format("%s/%s.log", RolePaths.LOGS_ROOT, name);
    }

    public static final TrogdorDaemonType AGENT = new TrogdorDaemonType(
        "agent",
        "trogdorAgent",
        RolePaths.TROGDOR_AGENT_ROOT,
        "org.apache.kafka.trogdor.agent.Agent");

    public static final TrogdorDaemonType COORDINATOR = new TrogdorDaemonType(
        "coordinator",
        "trogdorCoordinator",
        RolePaths.TROGDOR_COORDIINATOR_ROOT,
        "org.apache.kafka.trogdor.coordinator.Coordinator");
}
