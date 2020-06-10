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

package org.apache.kafka.controller;

import kafka.server.KafkaConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class PropagationManagerTest {
    private static final Logger log =
        LoggerFactory.getLogger(PropagationManagerTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCalculateListenerNameWithControlPlaneListener() {
        KafkaConfig config = ControllerTestUtils.newKafkaConfig(0,
            KafkaConfig.ControlPlaneListenerNameProp(), "CONTR",
            KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
            KafkaConfig.ListenersProp(), "CONTR://localhost:9093,INTERNAL://localhost:9092",
            KafkaConfig.ListenerSecurityProtocolMapProp(), "CONTR:PLAINTEXT,INTERNAL:PLAINTEXT");
        assertEquals("CONTR", PropagationManager.calculateListenerName(config));
    }

    @Test
    public void testCalculateListenerNameWithInterBrokerListenerName() {
        KafkaConfig config = ControllerTestUtils.newKafkaConfig(0,
            KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL",
            KafkaConfig.ListenersProp(), "INTERNAL://localhost:9092",
            KafkaConfig.ListenerSecurityProtocolMapProp(), "INTERNAL:PLAINTEXT");
        assertEquals("INTERNAL", PropagationManager.calculateListenerName(config));
    }
}
