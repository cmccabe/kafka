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

import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public final class ControllerUtils {
    public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    /**
     * A function which logs an exception at WARN level.
     */
    public static class WarnOnFailure implements Function<Throwable, Void> {
        private final String what;
        private final Logger log;

        public WarnOnFailure(String what, Logger log) {
            this.what = what;
            this.log = log;
        }

        @Override
        public Void apply(Throwable throwable) {
            log.warn("{} failed", what, throwable);
            return null;
        }
    }

    /**
     * Wait for a future to complete.  Log an error message if it fails.
     *
     * @param log       The logger object to use.
     * @param future    The future to wait for.
     */
    public static void await(Logger log, CompletableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            log.warn("Unexpected interruption.", e);
        } catch (ExecutionException e) {
            log.warn("Unexpected error waiting for future.", e);
        }
    }

    private final static DecimalFormat NANOS_TO_FRACTIONAL_MILLIS_DF =
        new DecimalFormat("#.####");

    public static String nanosToFractionalMillis(long ns) {
        float ms = ns;
        ms /= 1000000;
        return NANOS_TO_FRACTIONAL_MILLIS_DF.format(ms);
    }
}
