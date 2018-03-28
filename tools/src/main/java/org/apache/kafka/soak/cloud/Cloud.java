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

package org.apache.kafka.soak.cloud;

import org.apache.kafka.soak.cluster.SoakNode;

import java.util.Map;

public interface Cloud extends AutoCloseable {

    /**
     * Start an instance.
     *
     * @param params        The instance settings, including IMAGE_ID, INSTANCE_TYPE,
     *                      KEY_NAME, and SECURITY_GROUP.
     * @return              The instance ID.
     * @throws Exception    If the instance could not be created.
     */
    String runInstance(Map<String, String> params) throws Exception;

    /**
     * Describe an instance.
     *
     * @param instanceId    The instance ID.
     * @return              The instance status, including PRIVATE_DNS and PUBLIC_DNS.
     * @throws Exception    If the instance could not be created.
     */
    Map<String, String> describeInstance(String instanceId) throws Exception;

    /**
     * Terminates instances.
     *
     * @param instanceIds   The instance IDs.
     *
     * @throws Exception    If any of the instances could not be terminated.
     */
    void terminateInstances(String... instanceIds) throws Throwable;

    /**
     * Run a remote command on a cloud node.
     *
     * @param node          The node to run the command on.
     *
     * @return              The command object.  You must call run() to initiate the session.
     */
    RemoteCommand remoteCommand(SoakNode node);
}
