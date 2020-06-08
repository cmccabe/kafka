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

package org.apache.kafka.controller;

import org.apache.kafka.common.requests.LeaderAndIsrResponse;

/**
 * Handles events from the PropagationManager.
 */
public interface PropagationManagerCallbackHandler {
    /**
     * Schedule a new PropagationManager check.
     *
     * The check may be scheduled to handle making a retransmission, or to
     * handle making the original transmission that is delayed to allow time for
     * message coalescing.
     *
     * @param delayNs   The delay in nanoseconds after which we should schedule
     *                  the check.
     */
    void schedulePropagationManagerCheck(long delayNs);

    /**
     * Handle a LeaderAndIsrResponse
     *
     * @param brokerId      The ID of the broker that responded.
     * @param response      The response object.
     */
    void handleLeaderAndIsrResponse(int brokerId, LeaderAndIsrResponse response);
}
