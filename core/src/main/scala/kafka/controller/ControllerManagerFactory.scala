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

package kafka.controller

import kafka.server.KafkaConfig
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.metrics.Metrics

case class ControllerManagerFactory(nodeId: Int,
      config: KafkaConfig,
      zkClient: KafkaZkClient,
      metrics: Metrics,
      threadNamePrefix: String,
      className: String = "org.apache.kafka.controller.KafkaControllerManager") {
  def build(): ControllerManager = {
    val managerClass = Class.forName(className)
    val method = managerClass.getMethod("create", ControllerManagerFactory.getClass)
    method.invoke(null, this).asInstanceOf[ControllerManager]
  }
}
