/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import org.apache.kafka.clients.admin._
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.KafkaFuture
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.protocol.ApiKeys
import org.easymock.EasyMock
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._

/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see KafkaAdminClientUnitTest for a unit test of the admin client.
 */
class KafkaAdminClientIntegrationTest extends KafkaServerTestHarness with Logging {
  val brokerCount = 3
  lazy val serverConfig = new Properties

  @Before
  override def setUp() {
    super.setUp
  }

  def createConfig() : util.HashMap[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config
  }

  def waitForTopics(client: AdminClient, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    while (true) {
      val topics = client.listTopics().names().get()
      if (expectedPresent.forall(topicName => topics.contains(topicName)) &&
        expectedMissing.forall(topicName => !topics.contains(topicName))) {
        return
      }
      Thread.sleep(1)
    }
  }

  def assertFutureExceptionTypeEquals(future: KafkaFuture[_], clazz: Class[_ <: Throwable]): Unit = {
    try {
      future.get()
      fail("Expected CompletableFuture.get to return an exception")
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause()
        assert(clazz.isInstance(cause),
          "Expected an exception of type " + clazz.getName + "; got type " + cause.getClass().getName)
    }
  }

  @Test(timeout=120000)
  def testCloseAdministrativeClient(): Unit = {
    val client = AdminClient.create(createConfig())
    client.close()
  }

  @Test(timeout=120000)
  def testListNodes(): Unit = {
    val client = AdminClient.create(createConfig())
    val brokerStrs = brokerList.split(",").toList.sorted
    var nodeStrs : List[String] = null
    do {
      var nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map ( node => s"${node.host}:${node.port}" ).toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
    client.close()
  }

  @Test(timeout=120000)
  def testCreateDeleteTopics(): Unit = {
    val client = AdminClient.create(createConfig())
    val newTopics : List[NewTopic] = List(
        new NewTopic("mytopic", 1, 1),
        new NewTopic("mytopic2", 1, 1))
    client.createTopics(newTopics.asJava,
      new CreateTopicsOptions().validateOnly(true)).all().get()
    waitForTopics(client, List(), List("mytopic", "mytopic2"))

    client.createTopics(newTopics.asJava).all().get()
    waitForTopics(client, List("mytopic", "mytopic2"), List())

    val results = client.createTopics(newTopics.asJava).results()
    assert(results.containsKey("mytopic"))
    assertFutureExceptionTypeEquals(results.get("mytopic"), classOf[TopicExistsException])
    assert(results.containsKey("mytopic2"))
    assertFutureExceptionTypeEquals(results.get("mytopic2"), classOf[TopicExistsException])

    val deleteTopics : Set[String] = Set("mytopic", "mytopic2")
    client.deleteTopics(deleteTopics.asJava).all().get()
    waitForTopics(client, List(), List("mytopic", "mytopic2"))

    client.close()
  }

  @Test(timeout=120000)
  def testGetAllBrokerVersions(): Unit = {
    val client = AdminClient.create(createConfig())
    val nodes = client.describeCluster().nodes().get()
    val nodesToVersions = client.apiVersions(nodes).all().get()
    val brokers = brokerList.split(",")
    assert(brokers.size == nodesToVersions.size())
    for ((node, brokerVersionInfo) <- nodesToVersions.asScala) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(s"Unknown host:port pair $hostStr in brokerVersionInfos", brokers.contains(hostStr))
      assertEquals(0, brokerVersionInfo.usableVersion(ApiKeys.API_VERSIONS))
    }
    client.close()
  }

  override def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties)
    cfgs.foreach { config =>
      config.setProperty(KafkaConfig.ListenersProp, s"${listenerName.value}://localhost:${TestUtils.RandomPort}")
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, listenerName.value)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${listenerName.value}:${securityProtocol.name}")
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true");
    }
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }
}
