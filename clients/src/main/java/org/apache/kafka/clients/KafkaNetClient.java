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

package org.apache.kafka.clients;

public class KafkaNetClient implements NetClient {
    @Override
    public void poll(long maxMs) {
    }

    @Override
    public KafkaFuture<NetResponse> send(NetRequest request) {
//        Create new InFlightRequest (or equivalent):
//        InFlightRequest inFlightRequest = new InFlightRequest(
//                header,
//                clientRequest.createdTimeMs(),
//                clientRequest.destination(),
//                clientRequest.callback(),
//                clientRequest.expectResponse(),
//                isInternalRequest,
//                request,
//                send,
//                now);

//        If the node we want to send to is ready, call:
//        selector.send(inFlightRequest.send);
//        Otherwise, queue our request for later.
    }

    @Override
    public void abort(NetRequest request) {
    }

    private void abortInternal(NetRequest request) {

    @Override
    public void close() {
    }
}
