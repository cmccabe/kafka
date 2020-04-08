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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class KafkaEventQueueTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateAndClose() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(new LogContext(), "testCreateAndClose");
        queue.close();
    }

    @Test
    public void testHandleEvents() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(new LogContext(), "testHandleEvents");
        AtomicInteger numEventsExecuted = new AtomicInteger(0);
        CompletableFuture<Integer> future1 = queue.prepend(
            () -> {
                assertEquals(1, numEventsExecuted.incrementAndGet());
                return 1;
            });
        CompletableFuture<Integer> future2 = queue.appendWithDeadline(
            Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(30),
            () -> {
                assertEquals(2, numEventsExecuted.incrementAndGet());
                return 2;
            });
        CompletableFuture<Integer> future3 = queue.append(
            () -> {
                assertEquals(3, numEventsExecuted.incrementAndGet());
                return 3;
            });
        assertEquals(Integer.valueOf(1), future1.get());
        assertEquals(Integer.valueOf(3), future3.get());
        assertEquals(Integer.valueOf(2), future2.get());
        queue.appendWithDeadline(
            Time.SYSTEM.nanoseconds() + TimeUnit.SECONDS.toNanos(30),
            () -> {
                assertEquals(4, numEventsExecuted.incrementAndGet());
                return 4;
            }).get();
        queue.shutdown(TimeUnit.SECONDS, 60);
        queue.close();
    }

    @Test
    public void testTimeouts() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(new LogContext(), "testTimeouts");
        AtomicInteger numEventsExecuted = new AtomicInteger(0);
        CompletableFuture<Integer> future1 = queue.append(
            () -> {
                assertEquals(1, numEventsExecuted.incrementAndGet());
                return 1;
            });
        CompletableFuture<Integer> future2 = queue.append(
            () -> {
                assertEquals(2, numEventsExecuted.incrementAndGet());
                Time.SYSTEM.sleep(1);
                return 2;
            });
        CompletableFuture<Integer> future3 = queue.appendWithDeadline(
            Time.SYSTEM.nanoseconds() + 1,
            () -> {
                numEventsExecuted.incrementAndGet();
                return 3;
            });
        CompletableFuture<Integer> future4 = queue.append(
            () -> {
                numEventsExecuted.incrementAndGet();
                return 4;
            });
        assertEquals(Integer.valueOf(1), future1.get());
        assertEquals(Integer.valueOf(2), future2.get());
        assertEquals(Integer.valueOf(4), future4.get());
        assertEquals(TimeoutException.class,
            assertThrows(ExecutionException.class,
                () -> future3.get()).getCause().getClass());
        queue.close();
        assertEquals(3, numEventsExecuted.get());
    }

    @Test
    public void testClearException() throws Exception {
        KafkaEventQueue queue =
            new KafkaEventQueue(new LogContext(), "testClearException");
        final CountDownLatch setupLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        queue.append(() -> {
            try {
                setupLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(queue.append(() -> {
                failed.set(true);
                return null;
            }));
        }
        CompletableFuture<Integer> clearFuture = queue.clearAndEnqueue(
            new RuntimeException("cleared"), () -> 123);
        setupLatch.countDown();
        assertFalse(failed.get());
        for (CompletableFuture<Void> future : futures) {
            assertEquals(RuntimeException.class,
                assertThrows(ExecutionException.class,
                    () -> future.get()).getCause().getClass());
        }
        assertEquals(Integer.valueOf(123), clearFuture.get());
        queue.close();
    }
}
