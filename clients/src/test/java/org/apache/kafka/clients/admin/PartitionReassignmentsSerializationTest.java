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
package org.apache.kafka.clients.admin;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PartitionReassignmentsSerializationTest {
    private void testStringRoundTrip(String text) throws Exception {
        PartitionReassignments object = KafkaAdminClient.JSON_SERDE.
            readValue(text, PartitionReassignments.class);
        String text2 = KafkaAdminClient.JSON_SERDE.
            writerWithDefaultPrettyPrinter().writeValueAsString(object);
        System.out.printf("WATERMELON%n%s", text2);
        assertEquals(text, text2);
    }

    private String lines(String... lines) {
        return String.join(String.format("%n"), lines);
    }

    @Test
    public void testDeserializeReassignment1() throws Exception {
        String text = lines("{",
            "  \"version\" : 1,",
            "  \"partitions\" : [ {",
            "    \"topic\" : \"foo\",",
            "    \"partition\" : 1,",
            "    \"replicas\" : [ 1000, 1001, 1002 ]",
            "  } ]",
            "}");
        PartitionReassignments reassignment1 = KafkaAdminClient.JSON_SERDE.
            readValue(text, PartitionReassignments.class);
        assertEquals(1, reassignment1.version());
        assertEquals(1, reassignment1.partitions().size());
        assertEquals("foo", reassignment1.partitions().get(0).topic());
        assertEquals(1, reassignment1.partitions().get(0).partition());
        assertEquals(Arrays.asList(1000, 1001, 1002),
            reassignment1.partitions().get(0).replicas());
        testStringRoundTrip(text);
    }

    @Test
    public void testDeserializeReassignment2() throws Exception {
        String text = lines("{",
            "  \"version\" : 1,",
            "  \"partitions\" : [ {",
            "    \"topic\" : \"bar\",",
            "    \"partition\" : 1,",
            "    \"replicas\" : [ 1002, 1000, 1001 ]",
            "  }, {",
            "    \"topic\" : \"baz\",",
            "    \"partition\" : 2,",
            "    \"replicas\" : [ 1000 ],",
            "    \"logDirs\" : [ \"/a/b/c\" ]",
            "  } ]",
            "}");
        PartitionReassignments reassignment2 = KafkaAdminClient.JSON_SERDE.
            readValue(text, PartitionReassignments.class);
        assertEquals(1, reassignment2.version());
        assertEquals(2, reassignment2.partitions().size());
        assertEquals("bar", reassignment2.partitions().get(0).topic());
        assertEquals(1, reassignment2.partitions().get(0).partition());
        assertEquals(Arrays.asList(1002, 1000, 1001),
            reassignment2.partitions().get(0).replicas());
        assertFalse(reassignment2.partitions().get(0).logDirs().isPresent());
        assertEquals("baz", reassignment2.partitions().get(1).topic());
        assertEquals(2, reassignment2.partitions().get(1).partition());
        assertEquals(Arrays.asList(1000),
            reassignment2.partitions().get(1).replicas());
        assertEquals(Arrays.asList("/a/b/c"),
            reassignment2.partitions().get(1).logDirs().get());
        testStringRoundTrip(text);
    }
}
