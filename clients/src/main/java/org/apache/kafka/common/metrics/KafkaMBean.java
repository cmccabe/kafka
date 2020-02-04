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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Register metrics in JMX as dynamic mbeans based on the metric names
 */
class KafkaMbean implements DynamicMBean {
    private static final Logger log = LoggerFactory.getLogger(KafkaMbean.class);
    private final ObjectName objectName;
    private final Map<String, KafkaMetric> metrics;

    /**
     * @param metricName
     * @return standard JMX MBean name in the following format domainName:type=metricType,key1=val1,key2=val2
     */
    static String getMBeanName(String prefix, MetricName metricName) {
        StringBuilder mBeanName = new StringBuilder();
        mBeanName.append(prefix);
        mBeanName.append(":type=");
        mBeanName.append(metricName.group());
        for (Map.Entry<String, String> entry : metricName.tags().entrySet()) {
            if (entry.getKey().length() <= 0 || entry.getValue().length() <= 0)
                continue;
            mBeanName.append(",");
            mBeanName.append(entry.getKey());
            mBeanName.append("=");
            mBeanName.append(Sanitizer.jmxSanitize(entry.getValue()));
        }
        return mBeanName.toString();
    }

    KafkaMbean(String mbeanName) throws MalformedObjectNameException {
        this.metrics = new ConcurrentHashMap<>();
        this.objectName = new ObjectName(mbeanName);
    }

    public ObjectName name() {
        return objectName;
    }

    boolean addMetric(String name, KafkaMetric metric) {
        return this.metrics.putIfAbsent(name, metric) == null;
    }

    @Override
    public Object getAttribute(String name) throws AttributeNotFoundException {
        KafkaMetric metric = this.metrics.get(name);
        if (metric == null) {
            throw new AttributeNotFoundException("Could not find attribute " + name);
        }
        return metric.metricValue();
    }

    @Override
    public AttributeList getAttributes(String[] names) {
        AttributeList list = new AttributeList();
        for (String name : names) {
            try {
                list.add(new Attribute(name, getAttribute(name)));
            } catch (Exception e) {
                log.warn("Error getting JMX attribute '{}'", name, e);
            }
        }
        return list;
    }

    KafkaMetric removeAttribute(String name) {
        return this.metrics.remove(name);
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        List<MBeanAttributeInfo> output = new ArrayList<>();
        int i = 0;
        // It is safe to iterate over the ConcurrentHashMap while modifications are made to it.
        // The modifications may or may not be reflected in our iteration.
        for (Iterator<Map.Entry<String, KafkaMetric>> iter = metrics.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, KafkaMetric> entry = iter.next();
            String attribute = entry.getKey();
            KafkaMetric metric = entry.getValue();
            output.add(new MBeanAttributeInfo(attribute,
                double.class.getName(),
                metric.metricName().description(),
                true,
                false,
                false));
            i++;
        }
        MBeanAttributeInfo[] outputArray = output.toArray(new MBeanAttributeInfo[output.size()]);
        return new MBeanInfo(this.getClass().getName(), "", outputArray, null, null, null);
    }

    @Override
    public Object invoke(String name, Object[] params, String[] sig) {
        throw new UnsupportedOperationException("Set not allowed.");
    }

    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("Set not allowed.");
    }

    @Override
    public AttributeList setAttributes(AttributeList list) {
        throw new UnsupportedOperationException("Set not allowed.");
    }
}
