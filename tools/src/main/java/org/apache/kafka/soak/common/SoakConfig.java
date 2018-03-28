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

package org.apache.kafka.soak.common;

public final class SoakConfig {
    /**
     * Configures the ssh identity file to use.
     * If this is not set, no identity file will be used.
     */
    public static final String SSH_IDENTITY_FILE_KEY = "sshIdentityFile";

    /**
     * Configures the ssh username to use.
     * If this is not set, no username will be specified.
     */
    public static final String SSH_USER_KEY = "sshUser";

    /**
     * Configures the ssh port to use.
     * If this is not set, the system default will be used.
     */
    public static final String SSH_PORT_KEY = "sshPort";

    /**
     * Configures the cloud image ID to use.
     */
    public static final String IMAGE_ID = "imageId";

    /**
     * Configures the cloud instance type to use.
     */
    public static final String INSTANCE_TYPE = "instanceType";

    /**
     * Configures the private DNS address of a node.
     */
    public static final String PRIVATE_DNS = "privateDns";

    /**
     * Configures the public DNS address of a node.
     */
    public static final String PUBLIC_DNS = "publicDns";

    /**
     * Configures whether to use the internal DNS address
     * to communicate with a node.  This is a good idea if you
     * are running the soak tool inside the cloud.
     */
    public static final String INTERNAL_DNS_KEY = "internalDns";

    /**
     * The default for INTERNAL_DNS_KEY.  It defaults to false,
     * since most people do not run SoakTool inside the cloud.
     */
    public static final boolean INTERNAL_DNS_DEFAULT = false;
};
