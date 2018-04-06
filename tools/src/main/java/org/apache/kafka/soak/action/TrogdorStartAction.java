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

package org.apache.kafka.soak.action;

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

import static org.apache.kafka.soak.action.ActionPaths.TROGDOR_CONF_SUFFIX;
import static org.apache.kafka.soak.action.ActionPaths.TROGDOR_START_SCRIPT;

public class TrogdorStartAction extends Action  {
    private final TrogdorDaemonType daemonType;

    public TrogdorStartAction(TrogdorDaemonType daemonType,
            String scope) {
        super(new ActionId(daemonType.startType(), scope),
                new ActionId[]{},
                new ActionId[]{});
        this.daemonType = daemonType;
    }

    @Override
    public void call(final SoakCluster cluster, final SoakNode node) throws Throwable {
        File configFile = null, log4jFile = null;
        try {
            configFile = writeTrogdorConfig(cluster, node);
            log4jFile = writeTrogdorLog4j(cluster, node);
            SoakUtil.killJavaProcess(cluster, node, daemonType.className(), false);
            cluster.cloud().remoteCommand(node).args(createSetupPathsCommandLine(daemonType)).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(configFile.getAbsolutePath(),
                daemonType.propertiesPath()).mustRun();
            cluster.cloud().remoteCommand(node).syncTo(log4jFile.getAbsolutePath(),
                daemonType.log4jConfPath()).mustRun();
            cluster.cloud().remoteCommand(node).args(
                runDaemonCommandLine(daemonType, node.nodeName())).mustRun();
        } finally {
            SoakUtil.deleteFileOrLog(node.log(), configFile);
            SoakUtil.deleteFileOrLog(node.log(), log4jFile);
        }
        SoakUtil.waitFor(5, 30000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return 0 == cluster.cloud().remoteCommand(node).args(
                    SoakUtil.checkJavaProcessStatusArgs(daemonType.className())).run();
            }
        });
    }

    public static String[] createSetupPathsCommandLine(TrogdorDaemonType daemonType) {
        return new String[] {"-n", "--", "rm", "-f", daemonType.logPath(), "&&",
            "sudo", "mkdir", "-p", daemonType.root(), ActionPaths.LOGS_ROOT, "&&",
            "sudo", "chown", "`whoami`", daemonType.root(), "&&",
            "mkdir", "-p", daemonType.root() + TROGDOR_CONF_SUFFIX};
    }

    public static String[] runDaemonCommandLine(TrogdorDaemonType daemonType, String nodeName) {
        return new String[] {"-n", "--", "nohup", "env",
            String.format("KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"",
                daemonType.log4jConfPath()),
            TROGDOR_START_SCRIPT, daemonType.name(), "--" + daemonType.name() + ".config",
            daemonType.propertiesPath(), "--node-name", nodeName, "&>", "/dev/null", "&"};
    }

    private File writeTrogdorConfig(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(),
                String.format("trogdor-%s-%d.conf", daemonType.name(), node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write("{%n");
            osw.write("  \"platform\": \"org.apache.kafka.trogdor.basic.BasicPlatform\",%n");
            osw.write("  \"nodes\": {%n");
            String prefix = "%n";
            for (Map.Entry<String, SoakNode> entry : cluster.nodes().entrySet()) {
                String nodeName = entry.getKey();
                SoakNode soakNode = entry.getValue();
                osw.write(String.format("%s    \"%s\": {%n", prefix, nodeName));
                prefix = ",%n";
                osw.write(String.format("      \"hostname\": \"%s\",%n",
                    soakNode.spec().privateDns()));
                osw.write("      \"trogdor.agent.port\": 8888%n");
                osw.write("    }");
            }
            osw.write("%n");
            osw.write("  }%n");
            osw.write("}%n");
            success = true;
            return file;
        } finally {
            Utils.closeQuietly(osw, "temporary trogdor agent file OutputStreamWriter");
            Utils.closeQuietly(fos, "temporary trogdor agent file FileOutputStream");
            if (!success) {
                SoakUtil.deleteFileOrLog(node.log(), file);
            }
        }
    }

    private File writeTrogdorLog4j(SoakCluster cluster, SoakNode node) throws IOException {
        File file = null;
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        boolean success = false;
        try {
            file = new File(cluster.env().outputDirectory(),
                String.format("trogdor-%s-log4j-%d.properties",
                    daemonType.name(), node.nodeIndex()));
            fos = new FileOutputStream(file, false);
            osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            osw.write("log4j.rootLogger=WARN, kafkaAppender%n");
            osw.write("log4j.appender.kafkaAppender=org.apache.log4j.DailyRollingFileAppender%n");
            osw.write("log4j.appender.kafkaAppender.DatePattern='.'yyyy-MM-dd-HH%n");
            osw.write(String.format("log4j.appender.kafkaAppender.File=%s%n",
                daemonType.logPath()));
            osw.write("log4j.appender.kafkaAppender.layout=org.apache.log4j.PatternLayout%n");
            osw.write("log4j.appender.kafkaAppender.layout.ConversionPattern=[%d] %p %m (%c)%n%n");
            osw.write("log4j.logger.org.apache.kafka=DEBUG%n");
            osw.write("%n");
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
