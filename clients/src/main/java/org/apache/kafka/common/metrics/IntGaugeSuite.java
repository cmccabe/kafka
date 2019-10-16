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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Manages a suite of integer Gauges.
 */
public class IntGaugeSuite implements AutoCloseable {
    private final Logger log;
    private final String suiteName;
    private final Metrics metrics;
    private final Object lock;
    private final Function<String, MetricName> metricNameCalculator;
    private final int maxEntries;
    private final Map<String, StoredIntGauge> map;
    private boolean closed;

    private class StoredIntGauge implements Gauge<Integer> {
        private final MetricName metricName;
        int value;

        StoredIntGauge(MetricName metricName) {
            this.metricName = metricName;
            this.value = 0;
        }

        @Override
        public Integer value(MetricConfig config, long now) {
            synchronized (lock) {
                return value;
            }
        }
    }

    public IntGaugeSuite(Logger log,
                         String suiteName,
                         Metrics metrics,
                         Object lock,
                         Function<String, MetricName> metricNameCalculator,
                         int maxEntries) {
        this.log = log;
        this.suiteName = suiteName;
        this.metrics = metrics;
        this.lock = lock;
        this.metricNameCalculator = metricNameCalculator;
        this.maxEntries = maxEntries;
        this.map = new HashMap<>(1);
        this.closed = false;
    }

    public void increment(String shortName) {
        synchronized (lock) {
            if (closed) {
                log.warn("{}: Attempted to increment {}, but the GaugeSuite was closed.",
                    suiteName, shortName);
                return;
            }
            StoredIntGauge gauge = map.get(shortName);
            if (gauge == null) {
                if (map.size() == maxEntries) {
                    log.warn("{}: Attempted to increment {}, but there are already {} entries.",
                        suiteName, shortName, maxEntries);
                    return;
                }
                MetricName metricName = metricNameCalculator.apply(shortName);
                gauge = new StoredIntGauge(metricName);
                metrics.addMetric(metricName, gauge);
                map.put(shortName, gauge);
            }
            gauge.value++;
        }
    }

    public void decrement(String shortName) {
        synchronized (lock) {
            if (closed) {
                log.warn("{}: Attempted to decrement {}, but the GaugeSuite was closed.",
                    suiteName, shortName);
                return;
            }
            StoredIntGauge gauge = map.get(shortName);
            if (gauge == null) {
                log.warn("{}: Attempted to decrement {}, but no such metric was registered.",
                    suiteName, shortName);
            } else {
                gauge.value--;
                if (gauge.value == 0) {
                    metrics.removeMetric(gauge.metricName);
                    map.remove(shortName);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (!closed) {
                closed = true;
                for (StoredIntGauge gauge : map.values()) {
                    metrics.removeMetric(gauge.metricName);
                }
                map.clear();
            }
        }
    }
}
