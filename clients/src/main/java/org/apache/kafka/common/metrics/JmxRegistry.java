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
public class JmxRegistry<B> implements KafkaBeanRegistry<B> {
    public final static JmxRegistry INSTANCE = new JmxRegistry();
    private final Map<ObjectName, ReferenceCountedBean<B>> beans;

    private static class ReferenceCountedBean<B> {
        final B bean;
        int refcount;

        ReferenceCountedBean(B bean) {
            this.bean = bean;
        }
    }

    private JmxRegistry() {
        this.beans = new HashMap<>();
    }

    @Override
    public synchronized B register(ObjectName name, B bean) {
        ReferenceCountedBean<B> existing = beans.get(name);
        if (existing == null) {

        }


    }

    //@Override
    public synchronized B eregister(String prefix, KafkaMetric metric) {
        try {
            MetricName metricName = metric.metricName();
            String name = KafkaMbean.getMBeanName(prefix, metricName);
            KafkaMbean mbean = mbeans.get(name);
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (mbean == null) {
                mbean = new KafkaMbean(name);
                mbean.setAttribute(metricName.name(), metric);
                server.registerMBean(mbean, mbean.name());
                mbeans.put(name, mbean);
            } else {
                mbean.setAttribute(metricName.name(), metric);
                server.unregisterMBean(mbean.name());
                boolean success = false;
                try {
                    server.registerMBean(mbean, mbean.name());
                    success = true;
                } finally {
                    if (!success) {
                        mbeans.remove(name);
                    }
                }
            }
            mbean.addReference();
        } catch (JMException e) {
            throw new KafkaException("Error creating mbean attribute for metricName :" + metric.metricName(), e);
        }
    }

    @Override
    public synchronized void unregister(String prefix, KafkaMetric metric) {
        try {
            MetricName metricName = metric.metricName();
            String name = KafkaMbean.getMBeanName(prefix, metricName);
            KafkaMbean mbean = mbeans.get(name);
            if (mbean == null) {
                return;
            }
            mbean.removeAttribute(metricName.name());
            mbean.metrics.isEmpty()

        }
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            if (server.isRegistered(mbean.name()))
                server.unregisterMBean(mbean.name());
        } catch (JMException e) {
            throw new KafkaException("Error unregistering mbean", e);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (LOCK) {
            MetricName metricName = metric.metricName();
            String mBeanName = getMBeanName(prefix, metricName);
            KafkaMbean mbean = removeAttribute(metric, mBeanName);
            if (mbean != null) {
                if (mbean.metrics.isEmpty()) {
                    unregister(mbean);
                    mbeans.remove(mBeanName);
                } else
                    reregister(mbean);
            }
        }
    }

    private KafkaMbean removeAttribute(KafkaMetric metric, String mBeanName) {
        MetricName metricName = metric.metricName();
        KafkaMbean mbean = this.mbeans.get(mBeanName);
        if (mbean != null)
            mbean.removeAttribute(metricName.name());
        return mbean;
    }
}
