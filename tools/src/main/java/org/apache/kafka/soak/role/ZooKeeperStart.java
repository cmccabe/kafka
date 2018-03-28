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
import java.util.concurrent.Callable;

import static org.apache.kafka.soak.role.RoleDependencies.LINUX_SETUP;
import static org.apache.kafka.soak.role.RoleDependencies.RSYNC_SRC;
import static org.apache.kafka.soak.role.RoleDependencies.ZOOKEEPER_START;

public class ZooKeeperStart extends Action  {
    public ZooKeeperStart(String nodeName) {
        super(ZOOKEEPER_START,
            nodeName,
            new String[] {
                "?" +LINUX_SETUP + ":" + nodeName,
                RSYNC_SRC + ":" + nodeName
            },
            new String[] {ZOOKEEPER_START});
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            configFile = writeZooKeeperConfig(cluster, node);
            log4jFile = writeZooKeeperLog4j(cluster, node);
            SoakUtil.killJavaProcess(cluster, node, ZooKeeperRole.ZOOKEEPER_CLASS_NAME, false);
            cluster.cloud().remoteCommand(node).args(createSetupPathsCommandLine()).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                RolePaths.ZK_PROPERTIES).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(log4jFile.getAbsolutePath(),
                RolePaths.ZK_LOG4J).mustRun();
            cluster.cloud().remoteCommand(node).args(createRunDaemonCommandLine()).mustRun();
        } finally {
            SoakUtil.deleteFileOrLog(node.log(), configFile);
            SoakUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        SoakUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == cluster.cloud().remoteCommand(node).args(
                    SoakUtil.checkJavaProcessStatusArgs(ZooKeeperRole.ZOOKEEPER_CLASS_NAME)).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine() {
        return new String[]{"-n", "--",
            "rm", "-rf", RolePaths.ZK_OPLOGS, RolePaths.ZK_LOGS, "&&",
            "sudo", "mkdir", "-p", RolePaths.ZK_ROOT, RolePaths.KAFKA_SRC, "&&",
            "sudo", "chown", "`whoami`", RolePaths.ZK_ROOT, RolePaths.KAFKA_SRC, "&&",
            "mkdir", "-p", RolePaths.ZK_ROOT, RolePaths.ZK_CONF, RolePaths.ZK_OPLOGS, RolePaths.ZK_LOGS};
    }

    public static String[] createRunDaemonCommandLine() {
        return new String[] {"nohup", "env", "KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:" + RolePaths.ZK_LOG4J + "\"",
            RolePaths.ZK_START_SCRIPT, RolePaths.ZK_PROPERTIES, "&>", "/dev/null", "&"};
    }

    private File writeZooKeeperConfig(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.logPath(), String.format("zookeeper-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write(String.format("dataDir=%s\n", RolePaths.ZK_OPLOGS));
            osw.write("clientPort=2181\n");
            osw.write("maxClientCnxns=0\n");
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary ZooKeeper config file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary ZooKeeper config file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    static File writeZooKeeperLog4j(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.logPath(), String.format("zookeeper-log4j-%d.properties",
                node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write("log4j.rootLogger=INFO, kafkaAppender\n");
            osw.write("log4j.appender.kafkaAppender=org.apache.log4j.DailyRollingFileAppender\n");
            osw.write("log4j.appender.kafkaAppender.DatePattern='.'yyyy-MM-dd-HH\n");
            osw.write(String.format("log4j.appender.kafkaAppender.File=%s/server.log\n", RolePaths.ZK_LOGS));
            osw.write("log4j.appender.kafkaAppender.layout=org.apache.log4j.PatternLayout\n");
            osw.write("log4j.appender.kafkaAppender.layout.ConversionPattern=[%d] %p %m (%c)%n\n");
            osw.write("\n");
            osw.write("log4j.logger.org.I0Itec.zkclient.ZkClient=INFO\n");
            osw.write("log4j.logger.org.apache.zookeeper=INFO\n");
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
};
