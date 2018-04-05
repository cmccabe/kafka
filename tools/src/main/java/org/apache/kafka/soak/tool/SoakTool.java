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

package org.apache.kafka.soak.tool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.action.ActionScheduler;
import org.apache.kafka.soak.cloud.Ec2Cloud;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.common.SoakLog;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The soak command.
 */
public final class SoakTool {
    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private static final String SOAK_TEST_SPEC_PATH = "SOAK_TEST_SPEC_PATH";
    private static final String SOAK_AWS_SECURITY_GROUP = "SOAK_AWS_SECURITY_GROUP";
    private static final String SOAK_AWS_SECURITY_KEYPAIR = "SOAK_AWS_SECURITY_KEYPAIR";
    private static final String SOAK_TIMEOUT_SECONDS = "SOAK_TIMEOUT_SECONDS";
    private static final String SOAK_ACTION_FILTER = "SOAK_ACTION_FILTER";
    private static final String SOAK_TARGETS = "SOAK_TARGETS";
    private static final String SOAK_KAFKA_PATH = "SOAK_KAFKA_PATH";
    private static final String SOAK_OUTPUT_DIRECTORY = "SOAK_OUTPUT_DIRECTORY";
    private static final String SOAK_OUTPUT_DIRECTORY_DEFAULT = "/tmp/soak";

    private static String getEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            return val;
        }
        return defaultValue;
    }

    private static Integer getEnvInt(String name, Integer defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            try {
                return Integer.valueOf(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse value " + name +
                        " given for " + name, e);
            }
        }
        return defaultValue;
    }

    public static void main(String[] args) throws Throwable {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("soak-tool")
            .defaultHelp(true)
            .description("The Kafka soak cluster tool.");
            // .epilog

        parser.addArgument("-s", "--test-spec")
            .action(store())
            .type(String.class)
            .dest(SOAK_TEST_SPEC_PATH)
            .metavar(SOAK_TEST_SPEC_PATH)
            .setDefault(getEnv(SOAK_TEST_SPEC_PATH, ""))
            .help("The path to the soak cluster test specification file.");
        parser.addArgument("--sg")
            .action(store())
            .type(String.class)
            .dest(SOAK_AWS_SECURITY_GROUP)
            .metavar(SOAK_AWS_SECURITY_GROUP)
            .setDefault(getEnv(SOAK_AWS_SECURITY_GROUP, ""))
            .help("The AWS security group name to use.");
        parser.addArgument("--keypair")
            .action(store())
            .type(String.class)
            .dest(SOAK_AWS_SECURITY_KEYPAIR)
            .metavar(SOAK_AWS_SECURITY_KEYPAIR)
            .setDefault(getEnv(SOAK_AWS_SECURITY_KEYPAIR, ""))
            .help("The AWS keypair name to use.");
        parser.addArgument("-t", "--timeout")
            .action(store())
            .type(Integer.class)
            .dest(SOAK_TIMEOUT_SECONDS)
            .metavar(SOAK_TIMEOUT_SECONDS)
            .setDefault(getEnvInt(SOAK_TIMEOUT_SECONDS, 360))
            .help("The timeout in seconds.");
        parser.addArgument("-a", "--action-filter")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(SOAK_ACTION_FILTER)
            .metavar(SOAK_ACTION_FILTER)
            .setDefault(getEnv(SOAK_ACTION_FILTER, ActionScheduler.DEFAULT_ACTION_FILTER.toString()))
            .help("A regular expression which constrains the actions which are executed.");
        parser.addArgument("--kafka-path")
            .action(store())
            .type(String.class)
            .dest(SOAK_KAFKA_PATH)
            .metavar(SOAK_KAFKA_PATH)
            .setDefault(getEnv(SOAK_KAFKA_PATH, ""))
            .help("The path to the Kafka directory.");
        parser.addArgument("-o", "--output-directory")
            .action(store())
            .type(String.class)
            .dest(SOAK_OUTPUT_DIRECTORY)
            .metavar(SOAK_OUTPUT_DIRECTORY)
            .setDefault(getEnv(SOAK_OUTPUT_DIRECTORY, SOAK_OUTPUT_DIRECTORY_DEFAULT))
            .help("The output path to store logs, cluster files, and other outputs in.");
        parser.addArgument("target")
            .nargs("*")
            .action(store())
            .required(false)
            .dest(SOAK_TARGETS)
            .metavar(SOAK_TARGETS)
            .help("The target action(s) to run.");

        try {
            Namespace res = parser.parseArgsOrFail(args);
            SoakEnvironment env = new SoakEnvironment(
                    res.getString(SOAK_TEST_SPEC_PATH),
                    res.getString(SOAK_AWS_SECURITY_GROUP),
                    res.getString(SOAK_AWS_SECURITY_KEYPAIR),
                    res.getInt(SOAK_TIMEOUT_SECONDS),
                    res.getString(SOAK_ACTION_FILTER),
                    res.getString(SOAK_KAFKA_PATH),
                    res.getString(SOAK_OUTPUT_DIRECTORY));
            List<String> targets = res.<String>getList(SOAK_TARGETS);
            if (env.clusterPath().isEmpty()) {
                throw new RuntimeException("You must supply a cluster file path with -c.");
            }
            if (targets.isEmpty()) {
                parser.printHelp();
                System.exit(0);
            }
            SoakClusterSpec clusterSpec =
                SoakTool.JSON_SERDE.readValue(new File(env.clusterPath()), SoakClusterSpec.class);
            Files.createDirectories(Paths.get(env.outputDirectory()));
            SoakLog clusterLog = SoakLog.fromStdout("cluster");

            SoakReturnCode exitCode = SoakReturnCode.TOOL_FAILED;
            try (Ec2Cloud cloud = new Ec2Cloud()) {
                try (SoakCluster cluster = new SoakCluster(env, cloud, clusterLog, clusterSpec)) {
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            cluster.shutdownManager().shutdown();
                        }
                    });
                    try (ActionScheduler scheduler = cluster.createScheduler(targets)) {
                        scheduler.await(env.timeoutSecs());
                    }
                    cluster.shutdownManager().shutdown();
                    exitCode = cluster.shutdownManager().returnCode();
                }
            }
            System.exit(exitCode.code());
        } catch (Throwable exception) {
            System.out.printf("Exiting with exception: %s\n", Utils.fullStackTrace(exception));
            System.exit(1);
        }
    }
};
