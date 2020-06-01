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

import org.apache.kafka.common.message.MetadataState;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public final class KafkaController {
    private final ControllerLogContext logContext;
    private final Logger log;
    private final int controllerEpoch;
    private final BackingStore backingStore;
    private final EventQueue mainQueue;
    private final MetadataState state;

    KafkaController(ControllerLogContext logContext,
                    int controllerEpoch,
                    BackingStore backingStore,
                    EventQueue mainQueue,
                    MetadataState state) {
        this.logContext = logContext;
        this.log = new LogContext(logContext.logContext().logPrefix() +
            " [epoch " + controllerEpoch + "] ").logger(KafkaController.class);
        this.controllerEpoch = controllerEpoch;
        this.backingStore = backingStore;
        this.mainQueue = mainQueue;
        this.state = state;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }
}
