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

package kafka.controller;

import java.util
import java.util.{Map, Set}
import java.util.concurrent.CompletableFuture

import kafka.zk.BrokerInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ApiError

object ControllerManager {
  case class PartitionLeaderElectionResult(error: ApiError, leaderId: Int)
}

/**
 * Manages the Kafka Controller, a special node which handles some administrative
 * functions in the Kafka cluster.  All brokers have a controller manager, but only
 * one node can be the controller.
 */
trait ControllerManager extends AutoCloseable {
  import ControllerManager._

  /**
   * Start the controller manager.
   *
   * @return A future that is completed when we finish registering with ZK.
   */
  def start(brokerInfo: BrokerInfo): CompletableFuture[Void]

  /**
   * Start the asynchronous process of shutting down this controller manager.
   */
  def beginShutdown(): Unit

  /**
   * Update the broker info.
   *
   * @return A future that is completed when we finish changing the broker info.
   */
  def updateBrokerInfo(brokerInfo: BrokerInfo): CompletableFuture[Void]

  /**
   * Trigger an election on the specified partitions.
   *
   * @param timeoutMs The timeout in milliseconds to use.
   * @param parts     The partitions.
   * @return A future containing the results for each partition.
   */
  def electLeaders(timeoutMs: Int, parts: Set[TopicPartition]):
    CompletableFuture[Map[TopicPartition, PartitionLeaderElectionResult]]

  /**
   * Initiate a controlled shutdown of a broker.
   *
   * @param brokerId    The broker id.
   * @param brokerEpoch The broker epoch, or -1 if this is a pre-KIP-380 request.
   * @return A future containing the partitions that the broker
   *         still leads.
   */
  def controlledShutdown(brokerId: Int, brokerEpoch: Int):
    CompletableFuture[util.Set[TopicPartition]]
}
