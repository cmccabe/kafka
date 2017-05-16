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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class DynamicConsumer {
    private static final Logger log = LoggerFactory.getLogger(DynamicConsumer.class);

    static class TestConfig {
        final String propertiesFile;
        final String topicPrefix;
        final int messagesPerTopic;
        final String bootstrapServer;

        TestConfig(Namespace res) {
            this.propertiesFile = res.getString("propertiesFile");
            this.topicPrefix = res.getString("topicPrefix");
            this.messagesPerTopic = res.getInt("messagesPerTopic");
            this.bootstrapServer = res.getString("bootstrapServer");
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("dynamic-consumer")
            .defaultHelp(true)
            .description("A test tool for consuming from multiple topics.");
        parser.addArgument("--properties-file")
            .action(store())
            .required(false)
            .type(String.class)
            .dest("propertiesFile")
            .metavar("PROPERTIES_FILE")
            .help("a file containing consumer properties");
        parser.addArgument("--topic-prefix")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("topicPrefix")
            .metavar("TOPIC_PREFIX")
            .help("the prefix to use for topic names");
        parser.addArgument("--messages-per-topic")
            .action(store())
            .required(true)
            .type(Integer.class)
            .dest("messagesPerTopic")
            .metavar("MESSAGES_PER_TOPIC")
            .help("messages to consume per topic");
        parser.addArgument("--bootstrap-server")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("bootstrapServer")
            .metavar("BOOTSTRAP_SERVER")
            .help("The server(s) to use for bootstrapping");
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        TestConfig testConfig = new TestConfig(res);
        DynamicConsumer test = new DynamicConsumer(testConfig);
        try {
            test.run();
        } catch (Throwable t) {
            System.out.printf("FAILED: Caught exception %s%n%n", t.getMessage());
            t.printStackTrace();
            Exit.exit(1);
        }
    }

    private final static AtomicBoolean closing = new AtomicBoolean(false);

    private final TestConfig testConfig;

    private int curTopicIndex = 0;

    private int curTopicMessageCount = 0;

    private final Properties consumerProps;

    private final KafkaConsumer<byte[], byte[]> consumer;

    private final CountDownLatch throttleLatch = new CountDownLatch(1);

    DynamicConsumer(TestConfig testConfig) throws IOException {
        this.testConfig = testConfig;
        this.consumerProps = new Properties();
        if (testConfig.propertiesFile != null) {
            log.debug("loading properties file {}", testConfig.propertiesFile);
            try (InputStream propStream = new FileInputStream(testConfig.propertiesFile)) {
                consumerProps.load(propStream);
            }
        }
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 512);
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    private String curTopicName() {
        return String.format("%s%06d", testConfig.topicPrefix, curTopicIndex);
    }

    private void installSignalHandler() {
        Signal.handle(new Signal("USR1"), new SignalHandler() {
            public void handle(Signal sig) {
                closing.set(true);
            }
        });
        log.info("Installed signal handler for USR1.");
    }

    private void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                closing.set(true);
                throttleLatch.countDown();
            }
        });
        log.info("Installed shutdown hook.");
    }

    private void subscribeToCurTopic() {
        consumer.subscribe(Collections.singleton(curTopicName()));
        log.info("Subscribed to {}.", curTopicName());
    }

    void run() throws Exception {
        installSignalHandler();

        subscribeToCurTopic();
        System.out.println("{\"name\" : \"startup_complete\"");
        while (!closing.get()) {
            if (curTopicMessageCount >= testConfig.messagesPerTopic) {
                log.info("Received {} messages for {}.  Advancing topic counter.",
                    curTopicMessageCount, curTopicName());
                curTopicMessageCount = 0;
                curTopicIndex++;
                subscribeToCurTopic();
            }
            ConsumerRecords<byte[], byte[]> records = consumer.poll(0);
            for (Iterator<ConsumerRecord<byte[], byte[]>> iter = records.iterator(); iter.hasNext();) {
                iter.next();
                curTopicMessageCount++;
                log.debug("curTopicName={}, curTopicMessageCount={}.", curTopicName(), curTopicMessageCount);
            }
            throttleLatch.await(1, TimeUnit.MILLISECONDS);
        }
        consumer.close();
        System.out.println("{\"name\" : \"shutdown_complete\"");
    }
}
