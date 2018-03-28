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

package org.apache.kafka.soak.role;

import com.fasterxml.jackson.annotation.JsonCreator;

public class TrogdorAgentRole implements Role {
    @JsonCreator
    public TrogdorAgentRole() {
    }

    @Override
    public void setup(ActionScheduler.Builder bld, String nodeName) {
        bld.addAction(new TrogdorStart(nodeName, TrogdorDaemonType.AGENT));
    }

    @Override
    public void status(ActionScheduler.Builder bld, String nodeName,
                       RoleStatusCollector collector) {
        bld.addAction(new TrogdorStatus(nodeName, TrogdorDaemonType.AGENT,
                      collector));
    }

    @Override
    public void stop(ActionScheduler.Builder bld, String nodeName) {
        bld.addAction(new TrogdorStop(nodeName, TrogdorDaemonType.AGENT));
    }
};
