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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names
 */
public class JmxReporter implements MetricsReporter {
    private final String prefix;
    private final Map<String, KafkaMbean> mbeans;

    public JmxReporter() {
        this("");
    }

    /**
     * Create a JMX reporter that prefixes all metrics with the given string.
     */
    public JmxReporter(String prefix) {
        this.prefix = prefix;
        this.mbeans = new HashMap<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void metricChange(KafkaMetric metric) {
        String mbeanName = getMBeanName(prefix, metric.metricName());
        KafkaMbean mbean = null;
        synchronized (this) {
            // Look up the mbean associated with this metric (in our list of mbeans).
            mbean = mbeans.get(mbeanName);
            if (mbean == null) {
                mbean = new KafkaMbean(mbeanName);
            }
        }
        // Try to add this metric as a new attribute to that mbean.
        if (!mbean.addMetric(metric.metricName().name(), metric)) {
            return;
        }
        // If we succeeded, add an additional reference to our use of this mbean,
        // and then unregister and re-register the bean
        registry.addReference(mbeanName, mbean);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // remove this metric
    }

    public void close() {
        // remove all metrics
    }
}
