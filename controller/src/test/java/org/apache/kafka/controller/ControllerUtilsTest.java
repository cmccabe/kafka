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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;

public class ControllerUtilsTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testExceptionalFuture() throws Exception {
        RuntimeException exception = new RuntimeException("foo");
        try {
            ControllerUtils.exceptionalFuture(exception).get();
        } catch (ExecutionException e) {
            assertEquals(exception, e.getCause());
        }
    }

    @Test
    public void testNanosToFractionalMillis() throws Exception {
        assertEquals("123", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(123)));
        assertEquals("0", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(0)));
        assertEquals("1", ControllerUtils.
            nanosToFractionalMillis(TimeUnit.MILLISECONDS.toNanos(1)));
        assertEquals("1.2346", ControllerUtils.
            nanosToFractionalMillis(1234567));
    }
}
