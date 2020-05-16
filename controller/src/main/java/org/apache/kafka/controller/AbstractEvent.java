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

import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;

abstract class AbstractEvent<T> implements EventQueue.Event<T> {
    private final Logger log;
    final String name;

    AbstractEvent(Logger log) {
        this.log = log;
        this.name = getClass().getSimpleName();
    }

    @Override
    final public T run() throws Throwable {
        long startNs = Time.SYSTEM.nanoseconds();
        log.info("{}: starting", name);
        try {
            T value = execute();
            log.info("{}: finished after {} ms",
                name, executionTimeToString(startNs));
            return value;
        } catch (Throwable e) {
            if (e instanceof ExecutionException && e.getCause() != null) {
                e = e.getCause();
            }
            log.info("{}: caught {} after {} ms.", name, e.getClass().getSimpleName(),
                executionTimeToString(startNs));
            throw handleException(e);
        }
    }

    final private String executionTimeToString(long startNs) {
        long endNs = Time.SYSTEM.nanoseconds();
        return ControllerUtils.nanosToFractionalMillis(endNs - startNs);
    }

    public abstract T execute() throws Throwable;

    public abstract Throwable handleException(Throwable e) throws Throwable;
}
