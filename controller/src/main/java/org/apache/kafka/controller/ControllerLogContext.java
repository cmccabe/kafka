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

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

public final class ControllerLogContext {
    private final String threadNamePrefix;
    private final LogContext logContext;
    private final AtomicReference<Throwable> lastUnexpectedError;

    public static ControllerLogContext fromPrefix(String prefix) {
        return new ControllerLogContext(prefix + "_",
                new LogContext(String.format("[%s]", prefix)));
    }

    public ControllerLogContext(String threadNamePrefix, LogContext logContext) {
        this.threadNamePrefix = threadNamePrefix;
        this.logContext = logContext;
        this.lastUnexpectedError = new AtomicReference<>(null);
    }

    public String threadNamePrefix() {
        return threadNamePrefix;
    }

    public Logger createLogger(Class<?> clazz) {
        return logContext.logger(clazz);
    }

    public LogContext logContext() {
        return logContext;
    }

    public Throwable lastUnexpectedError() {
        return lastUnexpectedError.get();
    }

    public void setLastUnexpectedError(Logger log, String what, Throwable e) {
        log.error("Unexpected error: {}", what, e);
        lastUnexpectedError.set(e);
    }
}
