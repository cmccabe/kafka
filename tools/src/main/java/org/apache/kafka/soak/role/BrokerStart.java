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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.kafka.soak.role.BrokerRole.KAFKA_CLASS_NAME;
import static org.apache.kafka.soak.role.RoleDependencies.LINUX_SETUP;
import static org.apache.kafka.soak.role.RoleDependencies.RSYNC_SRC;
import static org.apache.kafka.soak.role.RoleDependencies.BROKER_START;
import static org.apache.kafka.soak.role.RoleDependencies.ZOOKEEPER_START;
import static org.apache.kafka.soak.role.RolePaths.KAFKA_LOGS;

/**
 * Starts the Kafka broker.
 */
public final class BrokerStart extends Action {
    private static final String DEFAULT_JVM_PERFORMANCE_OPTS =
        "-Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC " +
            "-XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 " +
            "-XX:G1HeapRegionSize=16M -XX:MinMetaspaceFreeRatio=50 " +
            "-XX:MaxMetaspaceFreeRatio=80";

    public BrokerStart(String nodeName) {
        super(BROKER_START,
            nodeName,
            new String[] {"?" +LINUX_SETUP + ":" + nodeName,
                RSYNC_SRC + ":" + nodeName,
                ZOOKEEPER_START + ":all"},
            new String[] {BROKER_START});
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            configFile = writeBrokerConfig(cluster, node);
            log4jFile = writeBrokerLog4j(cluster, node);
            SoakUtil.killJavaProcess(cluster, node, KAFKA_CLASS_NAME, true);
            cluster.cloud().remoteCommand(node).args(createSetupPathsCommandLine()).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                RolePaths.KAFKA_BROKER_PROPERTIES).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(log4jFile.getAbsolutePath(),
                RolePaths.KAFKA_BROKER_LOG4J).mustRun();
            cluster.cloud().remoteCommand(node).args(createRunDaemonCommandLine()).mustRun();
        } finally {
            SoakUtil.deleteFileOrLog(node.log(), configFile);
            SoakUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        SoakUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == cluster.cloud().remoteCommand(node).args(
                    SoakUtil.checkJavaProcessStatusArgs(KAFKA_CLASS_NAME)).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[] {"-n", "--",
            "rm", "-rf", RolePaths.KAFKA_OPLOGS, RolePaths.KAFKA_LOGS, "&&",
            "sudo", "mkdir", "-p", RolePaths.KAFKA_ROOT, "&&",
            "sudo", "chown", "`whoami`", RolePaths.KAFKA_ROOT, "&&",
            "mkdir", "-p", RolePaths.KAFKA_SRC, RolePaths.KAFKA_CONF,
            RolePaths.KAFKA_OPLOGS, RolePaths.KAFKA_LOGS};
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[] {"-n", "--", "nohup", "env",
            "KAFKA_JVM_PERFORMANCE_OPTS=\"" + DEFAULT_JVM_PERFORMANCE_OPTS + "\"",
            "KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:" + RolePaths.KAFKA_BROKER_LOG4J + "\" ",
            RolePaths.KAFKA_START_SCRIPT, RolePaths.KAFKA_BROKER_PROPERTIES, "&>", "/dev/null", "&"};
    }

    private File writeBrokerConfig(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.logPath(), String.format("broker-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("broker.id=%d\n", getBrokerId(cluster, node)));
            osw.write("listeners=PLAINTEXT://:9092\n");
            osw.write(String.format("advertised.host.name=%s\n",
                cluster.nodes().get(node.nodeName()).spec().privateDns()));
            osw.write("num.network.threads=3\n");
            osw.write("num.io.threads=8\n");
            osw.write("socket.send.buffer.bytes=102400\n");
            osw.write("socket.receive.buffer.bytes=102400\n");
            osw.write("socket.request.max.bytes=104857600\n");
            osw.write(String.format("log.dirs=%s\n", RolePaths.KAFKA_OPLOGS));
            osw.write("num.partitions=3\n");
            osw.write("num.recovery.threads.per.data.dir=1\n");
            osw.write("log.retention.bytes=104857600\n");
            osw.write(String.format("zookeeper.connect=%s\n", getZooKeeperConnectString(cluster)));
            osw.write("zookeeper.connection.timeout.ms=6000\n");
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary broker file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary broker file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    File writeBrokerLog4j(SoakCluster cluster,  SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.logPath(), String.format("broker-log4j-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write("log4j.rootLogger=INFO, kafkaAppender\n");
            osw.write("\n");
            writeDailyRollingFileAppender(osw, "kafkaAppender", "server.log");
            writeDailyRollingFileAppender(osw, "stateChangeAppender", "state-change.log");
            writeDailyRollingFileAppender(osw, "requestAppender", "kafka-request.log");
            writeDailyRollingFileAppender(osw, "cleanerAppender", "log-cleaner.log");
            writeDailyRollingFileAppender(osw, "controllerAppender", "controller.log");
            writeDailyRollingFileAppender(osw, "authorizerAppender", "kafka-authorizer.log");
            osw.write("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n");
            osw.write("log4j.logger.org.apache.zookeeper=INFO\n");
            osw.write("\n");
            osw.write("log4j.logger.kafka=INFO\n");
            osw.write("log4j.logger.org.apache.kafka=INFO\n");
            osw.write("\n");
            osw.write("log4j.logger.kafka.request.logger=WARN, requestAppender\n");
            osw.write("\n");
            osw.write("log4j.logger.kafka.controller=TRACE, controllerAppender\n");
            osw.write("log4j.additivity.kafka.controller=false\n");
            osw.write("\n");
            osw.write("log4j.logger.kafka.log.LogCleaner=INFO, cleanerAppender\n");
            osw.write("log4j.additivity.kafka.log.LogCleaner=false\n");
            osw.write("\n");
            osw.write("log4j.logger.state.change.logger=TRACE, stateChangeAppender\n");
            osw.write("log4j.additivity.state.change.logger=false\n");
            osw.write("\n");
            osw.write("log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender\n");
            osw.write("log4j.additivity.kafka.authorizer.logger=false\n");
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary broker log4j file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary broker log4j file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static void writeDailyRollingFileAppender(OutputStreamWriter osw, String appender,
                                              String logName) throws IOException {
        osw.write(String.format("log4j.appender.%s=org.apache.log4j.DailyRollingFileAppender\n", appender));
        osw.write(String.format("log4j.appender.%s.DatePattern='.'yyyy-MM-dd-HH\n", appender));
        osw.write(String.format("log4j.appender.%s.File=%s/%s\n", appender, KAFKA_LOGS, logName));
        osw.write(String.format("log4j.appender.%s.layout=org.apache.log4j.PatternLayout\n", appender));
        osw.write(String.format("log4j.appender.%s.", appender) +
            "layout.ConversionPattern=[%d] %p %m (%c)%n\n");
        osw.write("\n");
    }

    private int getBrokerId(SoakCluster cluster, SoakNode node) {
        for (Map.Entry<Integer, String> entry :
                cluster.nodesWithRole(BrokerRole.class).entrySet()) {
            if (entry.getValue().equals(node.nodeName())) {
                return entry.getKey();
            }
        }
        throw new RuntimeException("Node " + node.nodeName() + " does not have the broker role.");
    }

    static String getZooKeeperConnectString(SoakCluster cluster) {
        StringBuilder zkConnectBld = new StringBuilder();
        String prefix = "";
        for (String nodeName : cluster.nodesWithRole(ZooKeeperRole.class).values()) {
            zkConnectBld.append(prefix);
            prefix = ",";
            zkConnectBld.append(cluster.nodes().get(nodeName).spec().privateDns()).append(":2181");
        }
        return zkConnectBld.toString();
    }
}
