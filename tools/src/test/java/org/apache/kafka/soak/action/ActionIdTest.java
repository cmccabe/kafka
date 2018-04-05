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

package org.apache.kafka.soak.action;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ActionIdTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testParseId() throws Throwable {
        assertEquals(new ActionId("foo", ActionId.SCOPE_ALL),  ActionId.parse("foo"));
        testParseAndToString(new ActionId("foo", ActionId.SCOPE_ALL), "foo:all");
        assertEquals(new ActionId("bar", ActionId.SCOPE_ALL), ActionId.parse("bar"));
        testParseAndToString(new ActionId("bar", "node3"), "bar:node3");
    }

    private void testParseAndToString(ActionId actionId, String str) {
        assertEquals(actionId, ActionId.parse(str));
        assertEquals(str, actionId.toString());
    }

    @Test
    public void testActionIdEquals() throws Throwable {
        assertEquals(new ActionId("foo", "node1"),
            new ActionId("foo", "node1"));
        assertEquals(new ActionId("baz", "node3"),
            new ActionId("baz", "node3"));
        assertEquals(new ActionId("baz", ActionId.SCOPE_ALL),
            new ActionId("baz", ActionId.SCOPE_ALL));
    }

    @Test
    public void testHasGlobalScope() throws Throwable {
        assertFalse(new ActionId("foo", "node1").hasGlobalScope());
        assertTrue(new ActionId("foo", "all").hasGlobalScope());
        assertTrue(new ActionId("", "all").hasGlobalScope());
    }
};
