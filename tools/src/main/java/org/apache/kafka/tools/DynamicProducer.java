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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class DynamicProducer {
    private static final Logger log = LoggerFactory.getLogger(DynamicProducer.class);

    static class TestConfig {
        final String propertiesFile;
        final String topicPrefix;
        final int messagesPerTopic;
        final int maxMessagesPerMinute;
        final String bootstrapServer;
        final short replicasPerTopic;

        TestConfig(Namespace res) {
            this.propertiesFile = res.getString("propertiesFile");
            this.topicPrefix = res.getString("topicPrefix");
            this.messagesPerTopic = res.getInt("messagesPerTopic");
            this.maxMessagesPerMinute = res.getInt("maxMessagesPerMinute");
            this.bootstrapServer = res.getString("bootstrapServer");
            this.replicasPerTopic = res.getShort("replicasPerTopic");
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("dynamic-producer")
            .defaultHelp(true)
            .description("A test tool for producing to multiple topics.");
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
            .help("messages to producer per topic");
        parser.addArgument("--messages-per-minute")
            .action(store())
            .required(true)
            .type(Integer.class)
            .dest("maxMessagesPerMinute")
            .metavar("MAX_MESSAGES_PER_MINUTE")
            .help("the maximum number of messages per minute to produce");
        parser.addArgument("--bootstrap-server")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("bootstrapServer")
            .metavar("BOOTSTRAP_SERVER")
            .help("The server(s) to use for bootstrapping");
        parser.addArgument("--replicas-per-topic")
            .action(store())
            .required(true)
            .type(Short.class)
            .dest("replicasPerTopic")
            .metavar("REPLICAS_PER_TOPIC")
            .help("The number of replicas per topic to use for newly created topics.");
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
        DynamicProducer test = new DynamicProducer(testConfig);
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

    private long prevTimeMinutes = 0;

    private int messagesThisMinute = 0;

    private final Properties producerProps;

    private final KafkaProducer<byte[], byte[]> producer;

    private final AdminClient adminClient;

    private final CountDownLatch latch = new CountDownLatch(1);

    DynamicProducer(TestConfig testConfig) throws IOException {
        this.testConfig = testConfig;
        this.producerProps = new Properties();
        if (testConfig.propertiesFile != null) {
            log.debug("loading properties file {}", testConfig.propertiesFile);
            try (InputStream propStream = new FileInputStream(testConfig.propertiesFile)) {
                producerProps.load(propStream);
            }
        }
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        ByteArraySerializer serializer = new ByteArraySerializer();
        this.producer = new KafkaProducer<byte[], byte[]>(producerProps, serializer, serializer);
        this.adminClient = AdminClient.create(new AdminClientConfig(producerProps));
    }

    private String curTopicName() {
        return String.format("%s%06d", testConfig.topicPrefix, curTopicIndex);
    }

    private void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                closing.set(true);
                latch.countDown();
            }
        });
        log.info("Installed shutdown hook.");
    }

    private boolean canDescribeTopic(String topicName) {
        if (adminClient.describeTopics(Collections.singleton(topicName)).all().isCompletedExceptionally())
            return false;
        log.debug("Successfully described topic {}.", topicName);
        return true;
    }

    private void createTopic() throws Exception {
        String topicName = curTopicName();
        if (canDescribeTopic(topicName))
            return;
        adminClient.createTopics(Collections.singleton(
                new NewTopic(topicName, 1, testConfig.replicasPerTopic))).all();
        while (!canDescribeTopic(topicName) && !closing.get()) {
            latch.await(1, TimeUnit.MILLISECONDS);
        }
    }

    void run() throws Exception {
        installShutdownHook();

        System.out.println("{\"name\" : \"startup_complete\"");
        while (!closing.get()) {
            long curTimeMs = System.currentTimeMillis();
            long curTimeMinutes = TimeUnit.MILLISECONDS.toMinutes(curTimeMs);
            if (curTimeMinutes != prevTimeMinutes) {
                messagesThisMinute = 0;
                prevTimeMinutes = curTimeMinutes;
            } else if (messagesThisMinute > testConfig.maxMessagesPerMinute) {
                long remainderMs =
                    1000 - (curTimeMs - TimeUnit.MINUTES.toMillis(curTimeMinutes));
                latch.await(remainderMs, TimeUnit.MILLISECONDS);
            } else if (curTopicMessageCount >= testConfig.messagesPerTopic) {
                log.info("Sent {} messages for {}.  Advancing topic counter.",
                    curTopicMessageCount, curTopicName());
                curTopicMessageCount = 0;
                curTopicIndex++;
                createTopic();
            } else {
                byte[] key = curTopicName().getBytes(StandardCharsets.UTF_8);
                byte[] value = ByteBuffer.allocate(4).putInt(curTopicMessageCount).array();
                ProducerRecord record = new ProducerRecord(curTopicName(), key, value);
                curTopicMessageCount++;
                messagesThisMinute++;
                log.debug("curTopicName={}, curTopicMessageCount={}.", curTopicName(), curTopicMessageCount);
                producer.send(record);
            }
        }
        producer.close();
        System.out.println("{\"name\" : \"shutdown_complete\"");
    }
}
