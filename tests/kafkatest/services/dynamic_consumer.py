# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import time

import signal

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.verifiable_client import VerifiableClientMixin
from kafkatest.utils import is_int, is_int_with_prefix
from kafkatest.version import DEV_BRANCH

class DynamicConsumer(KafkaPathResolverMixin, BackgroundThreadService):
    """This service wraps org.apache.kafka.tools.DynamicConsumer for use in
    system testing.
    """

    JAVA_CLASS_NAME = "org.apache.kafka.tools.DynamicConsumer"
    PERSISTENT_ROOT = "/mnt/dynamic_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "dynamic_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "dynamic_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "dynamic_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "dynamic_consumer.properties")

    logs = {
        "dynamic_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "dynamic_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "dynamic_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, kafka, acks="all", log_level="INFO"):
        """
        :param kafka: The Kafka service to consume from.
        :param log_level: The log4j log level to use.
        """
        super(BackgroundThreadService, self).__init__(context, num_nodes = 1)
        self.kafka = kafka
        self.acks = acks
        self.log_level = log_level # Referenced in log4j.properties
        self.shutdown_complete = False

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % DynamicConsumer.PERSISTENT_ROOT, allow_fail=False)
        log_config = self.render('tools_log4j.properties', log_file=DynamicConsumer.LOG_FILE)
        node.account.create_file(DynamicConsumer.LOG4J_CONFIG, log_config)

        self.security_config = self.kafka.security_config.client_config(node=node)
        self.security_config.setup_node(node)

        prop_file_text = ("request.timeout.ms=%d" % 30000)
        prop_file_text += ("\nacks=%s" % self.acks)
        prop_file_text += ("\nacks=%s" % self.acks)
        self.logger.info("dynamic_consumer.properties:")
        self.logger.info(prop_file_text)
        node.account.create_file(DynamicConsumer.CONFIG_FILE, prop_file_text)

        cmd = self.start_cmd(node, idx)
        self.logger.debug("DynamicConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            line = line.strip()
            event = self.try_parse_json(line)
            if event is not None:
                name = event["name"]
                if name == "startup_complete":
                    self.logger.debug("Read startup_complete message.")
                elif name == "shutdown_complete":
                    self.shutdown_complete = True
                else:
                    raise Exception("%s: unknown event %s" % (str(node.account), event))

        if not self.shutdown_complete:
            raise Exception("%s: ssh session ended without a successful shutdown." % str(node.account))

    def start_cmd(self, node, idx):
        cmd  = "export LOG_DIR=%s;" % DynamicConsumer.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % DynamicConsumer.LOG4J_CONFIG
        cmd += self.path.script("kafka-run-class.sh", node) + DynamicConsumer.JAVA_CLASS_NAME
        cmd += " --properties-file %s" % DynamicConsumer.CONFIG_FILE
        cmd += " --topic-prefix mytopic_"
        cmd += " --messages-per-topic 1000"
        cmd += " --bootstrap-server %s" % (self.kafka.bootstrap_servers(self.security_config.security_protocol))
        cmd += " 2>> %s | tee -a %s &" % (DynamicConsumer.STDERR_CAPTURE, DynamicConsumer.STDOUT_CAPTURE)
        return cmd

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        if clean_shutdown:
            sig = signal.SIGTERM
        else:
            sig = signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

    def pids(self, node):
        """ :return: pid(s) for this client intstance on node """
        try:
            cmd = "jps | grep -i " + DynamicConsumer.JAVA_CLASS_NAME + " | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        self.kill_node(node, clean_shutdown=True, allow_fail=False)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None
