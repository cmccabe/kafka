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
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.common.SoakLog;
import org.apache.kafka.soak.role.ActionScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The soak command.
 */
public final class SoakTool {
    private static final Logger log = LoggerFactory.getLogger(SoakTool.class);

    private static final String UP_COMMAND = "up";
    private static final String UP_COMMAND_HELP = "Brings up a soak cluster.";
    private static final String SETUP_COMMAND = "setup";
    private static final String SETUP_COMMAND_HELP = "Sets up the soak cluster nodes.";
    private static final String STATUS_COMMAND = "status";
    private static final String STATUS_COMMAND_HELP = "Gets the status of a soak cluster.";
    private static final String SSH_COMMAND = "ssh";
    private static final String SSH_COMMAND_HELP = "Ssh to a soak cluster node.";
    private static final String STOP_COMMAND = "stop";
    private static final String STOP_COMMAND_HELP = "Stops the roles on the soak cluster.";
    private static final String DOWN_COMMAND = "down";
    private static final String DOWN_COMMAND_HELP = "Brings down the soak cluster nodes.";

    private static final String COMMAND = "command";
    private static final String TEST_SPEC = "test_spec";
    private static final String CLUSTER = "cluster";
    private static final String TIMEOUT_SECONDS = "timeout_seconds";
    private static final String ACTION_FILTER = "action_filter";
    private static final String ACTION_FILTER_HELP =
        "A regular expression which constrains the actions which are executed.";
    private static final String NODE_NAME = "node_name";
    private static final String SSH_COMMANDS = "ssh_commands";

    private static final String SOAK_ROOT_ENV = "SOAK_ROOT";
    private static final String SOAK_ROOT_DEFAULT = "/tmp/soak";
    private static final String KAFKA_PATH_ENV = "KAFKA_PATH";
    private static final String SECURITY_GROUP = "security_group";
    private static final String SECURITY_GROUP_ENV = "AWS_SOAK_SECURITY_GROUP";
    private static final String SECURITY_KEYPAIR = "security_keypair";
    private static final String SECURITY_KEYPAIR_ENV = "AWS_SOAK_SECURITY_KEYPAIR";

    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public static void main(String[] args) throws Throwable {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("soak-tool")
            .defaultHelp(true)
            .description("The Kafka soak cluster tool.")
            .epilog("This tool accepts some environment variables:\n" +
                "  AWS_ACCESS_KEY_ID: the AWS access key to use.\n" +
                "  AWS_SECRET_KEY: the AWS secret key to use.\n" +
                "  AWS_REGION: the AWS region to use.\n" +
                "  " + SECURITY_GROUP_ENV + ": the security group name to use.\n" +
                "  " + SECURITY_KEYPAIR_ENV + ": the security key pair name to use.\n" +
                "  " + SOAK_ROOT_ENV + ": the root directory under which to write log \n" +
                "    files and other output.  Defaults to " + SOAK_ROOT_DEFAULT);

        Subparsers subparsers = parser.addSubparsers().dest(COMMAND);
        subparsers.description(
            UP_COMMAND + ": " + UP_COMMAND_HELP + "\n" +
            SETUP_COMMAND + ": " + SETUP_COMMAND_HELP + "\n" +
            STATUS_COMMAND + ": " + STATUS_COMMAND_HELP + "\n" +
            SSH_COMMAND + ": " + SSH_COMMAND_HELP + "\n" +
            STOP_COMMAND + ": " + STOP_COMMAND_HELP + "\n" +
            DOWN_COMMAND + ": " + DOWN_COMMAND_HELP);

        Subparser upParser = subparsers.addParser(UP_COMMAND).
            description(UP_COMMAND_HELP);
        upParser.addArgument("-s", "--test-spec")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(TEST_SPEC)
            .metavar(TEST_SPEC)
            .help("The soak test specification file to use.");
        upParser.addArgument("--sg")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(SECURITY_GROUP)
            .metavar(SECURITY_GROUP)
            .setDefault(System.getenv(SECURITY_GROUP_ENV))
            .help("The security group name to use.");
        upParser.addArgument("--keypair")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(SECURITY_KEYPAIR)
            .metavar(SECURITY_KEYPAIR)
            .setDefault(System.getenv(SECURITY_KEYPAIR_ENV))
            .help("The keypair name to use.");
        upParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to output.");
        upParser.addArgument("-t", "--timeout")
            .action(store())
            .required(false)
            .type(Integer.class)
            .dest(TIMEOUT_SECONDS)
            .metavar(TIMEOUT_SECONDS)
            .setDefault(180)
            .help("The timeout in seconds.");

        Subparser setupParser = subparsers.addParser(SETUP_COMMAND).
            description(SETUP_COMMAND_HELP);
        setupParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to use.");
        setupParser.addArgument("-t", "--timeout")
            .action(store())
            .required(false)
            .type(Integer.class)
            .dest(TIMEOUT_SECONDS)
            .metavar(TIMEOUT_SECONDS)
            .setDefault(180)
            .help("The timeout in seconds.");
        setupParser.addArgument("-a", "--action-filter")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(ACTION_FILTER)
            .metavar(ACTION_FILTER)
            .setDefault(ActionScheduler.DEFAULT_ACTION_FILTER.toString())
            .help(ACTION_FILTER_HELP);

        Subparser statusParser = subparsers.addParser(STATUS_COMMAND).
            description(STATUS_COMMAND_HELP);
        statusParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to use.");
        statusParser.addArgument("-t", "--timeout")
            .action(store())
            .required(false)
            .type(Integer.class)
            .dest(TIMEOUT_SECONDS)
            .metavar(TIMEOUT_SECONDS)
            .setDefault(60)
            .help("The timeout in seconds.");
        statusParser.addArgument("-a", "--action-filter")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(ACTION_FILTER)
            .metavar(ACTION_FILTER)
            .setDefault(ActionScheduler.DEFAULT_ACTION_FILTER.toString())
            .help(ACTION_FILTER_HELP);

        Subparser sshParser = subparsers.addParser(SSH_COMMAND).
            description(SSH_COMMAND_HELP);
        sshParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to use.");
        sshParser.addArgument("-n", "--node")
            .action(Arguments.append())
            .required(true)
            .type(String.class)
            .dest(NODE_NAME)
            .metavar(NODE_NAME)
            .help("The node to use, or all to use all nodes.");
        sshParser.addArgument("command")
            .nargs("*")
            .action(store())
            .required(false)
            .dest(SSH_COMMANDS)
            .metavar(SSH_COMMANDS)
            .help("The ssh command to run.");

        Subparser stopParser = subparsers.addParser(STOP_COMMAND).
            description(STOP_COMMAND_HELP);
        stopParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to use.");
        stopParser.addArgument("-t", "--timeout")
            .action(store())
            .required(false)
            .type(Integer.class)
            .dest(TIMEOUT_SECONDS)
            .metavar(TIMEOUT_SECONDS)
            .setDefault(60)
            .help("The timeout in seconds.");
        stopParser.addArgument("-a", "--action-filter")
            .action(store())
            .required(false)
            .type(String.class)
            .dest(ACTION_FILTER)
            .metavar(ACTION_FILTER)
            .setDefault(ActionScheduler.DEFAULT_ACTION_FILTER.toString())
            .help(ACTION_FILTER_HELP);

        Subparser destroyParser = subparsers.addParser(DOWN_COMMAND).
            description(DOWN_COMMAND_HELP);
        destroyParser.addArgument("-c", "--cluster")
            .action(store())
            .required(true)
            .type(String.class)
            .dest(CLUSTER)
            .metavar(CLUSTER)
            .help("The cluster JSON file to use.");

        try {
            SoakEnvironment.Builder envBuilder = new SoakEnvironment.Builder().
                rootPath(System.getenv(SOAK_ROOT_ENV)).
                kafkaPath(System.getenv(KAFKA_PATH_ENV)).
                clusterLog(SoakLog.fromStdout(SoakLog.CLUSTER));
            if (envBuilder.rootPath() == null) {
                envBuilder.rootPath(SOAK_ROOT_DEFAULT);
            }
            if (envBuilder.kafkaPath() == null) {
                throw new RuntimeException(KAFKA_PATH_ENV + " must be set.");
            }

            Namespace res = parser.parseArgsOrFail(args);
            String command = res.getString(COMMAND);
            if (command.equals(UP_COMMAND)) {
                envBuilder.timeoutSecs(res.getInt(TIMEOUT_SECONDS));
                envBuilder.keyPair(res.getString(SECURITY_KEYPAIR));
                if (envBuilder.keyPair() == null) {
                    throw new RuntimeException("You must supply an AWS security keypair on " +
                        "the command line, or by setting " + SECURITY_KEYPAIR_ENV);
                }
                envBuilder.securityGroup(res.getString(SECURITY_GROUP));
                if (envBuilder.securityGroup() == null) {
                    throw new RuntimeException("You must supply an AWS security group on " +
                        "the command line, or by setting " + SECURITY_GROUP_ENV);
                }
                SoakUp.run(res.getString(TEST_SPEC), res.getString(CLUSTER), envBuilder.build());
            } else if (command.equals(SETUP_COMMAND)) {
                envBuilder.timeoutSecs(res.getInt(TIMEOUT_SECONDS));
                envBuilder.actionFilter(Pattern.compile(res.getString(ACTION_FILTER)));
                SoakSetup.run(res.getString(CLUSTER), envBuilder.build());
            } else if (command.equals(STATUS_COMMAND)) {
                envBuilder.timeoutSecs(res.getInt(TIMEOUT_SECONDS));
                envBuilder.actionFilter(Pattern.compile(res.getString(ACTION_FILTER)));
                SoakStatus.run(res.getString(CLUSTER), envBuilder.build());
            } else if (command.equals(SSH_COMMAND)) {
                SoakClusterCommand.run(res.getString(CLUSTER), envBuilder.build(),
                    res.<String>getList(NODE_NAME), res.<String>getList(SSH_COMMANDS));
            } else if (command.equals(STOP_COMMAND)) {
                envBuilder.timeoutSecs(res.getInt(TIMEOUT_SECONDS));
                envBuilder.actionFilter(Pattern.compile(res.getString(ACTION_FILTER)));
                SoakStop.run(res.getString(CLUSTER), envBuilder.build());
            } else if (command.equals(DOWN_COMMAND)) {
                SoakDown.run(res.getString(CLUSTER), envBuilder.build());
            } else {
                System.out.printf("Invalid command %s.  Type --help for help.", command);
                System.exit(1);
            }
        } catch (Throwable exception) {
            System.out.printf("Exiting with exception: %s\n", Utils.fullStackTrace(exception));
            System.exit(1);
        }
    }
};
