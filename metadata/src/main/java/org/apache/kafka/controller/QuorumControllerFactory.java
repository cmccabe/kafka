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

import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

public final class QuorumControllerFactory {
    private int nodeId = -1;
    private MetaLogManager metaLogManager = null;
    private String prefix = "";
    private LogContext logContext = null;
    private EventQueue eventQueue = null;

    public QuorumControllerFactory setNodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public QuorumControllerFactory setMetaLogManager(MetaLogManager metaLogManager) {
        this.metaLogManager = metaLogManager;
        return this;
    }

    public QuorumControllerFactory setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public QuorumControllerFactory setLogContext(LogContext logContext) {
        this.logContext = logContext;
        return this;
    }

    public QuorumControllerFactory setEventQueue(EventQueue eventQueue) {
        this.eventQueue = eventQueue;
        return this;
    }

    public QuorumController build() throws InterruptedException {
        EventQueue newEventQueue = null;
        try {
            if (nodeId == -1) {
                throw new RuntimeException("You must set a non-negative node id.");
            }
            if (metaLogManager == null) {
                throw new RuntimeException("You must set a metadata log manager.");
            }
            if (logContext == null) {
                logContext = createDefaultLogContext();
            }
            if (eventQueue == null) {
                eventQueue = newEventQueue = createDefaultEventQueue();
            }
            return new QuorumController(logContext.logger(QuorumController.class),
                    Time.SYSTEM,
                    eventQueue,
                    nodeId,
                    metaLogManager);
        } catch (Exception e) {
            if (newEventQueue != null) {
                newEventQueue.close();
            }
            throw e;
        }
    }

    private LogContext createDefaultLogContext() {
        return new LogContext(String.format("%s[Controller %d]", prefix, nodeId));
    }

    private EventQueue createDefaultEventQueue() {
        return new KafkaEventQueue(logContext, prefix);
    }
}
