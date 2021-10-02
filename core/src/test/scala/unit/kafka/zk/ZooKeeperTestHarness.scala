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

package kafka.zk

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.InetSocketAddress
import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture

import javax.security.auth.login.Configuration
import kafka.raft.KafkaRaftManager
import kafka.server.{BrokerServer, ControllerServer, KafkaConfig, KafkaRaftServer, MetaProperties}
import kafka.tools.StorageTool
import kafka.utils.{CoreUtils, Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Tag, TestInfo}

import scala.collection.{Seq, immutable}

@Tag("integration")
abstract class ZooKeeperTestHarness extends Logging {
  private var _isKRaftTest = false

  def isKRaftTest(): Boolean = _isKRaftTest

  def checkIsZKTest(): Unit = {
    if (_isKRaftTest) {
      throw new RuntimeException("This function can't be accessed when running the test " +
        "in KRaft mode. ZooKeeper mode is required.")
    }
  }

  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000 // Allows us to avoid ZK session expiration due to GC up to 2/3 * 15000ms = 10 secs
  val zkMaxInFlightRequests = Int.MaxValue

  protected def zkAclsEnabled: Option[Boolean] = None

  private var _zkClient: KafkaZkClient = null

  def zkClient: KafkaZkClient = {
    checkIsZKTest()
    _zkClient
  }

  private var _adminZkClient: AdminZkClient = null

  def adminZkClient: AdminZkClient = {
    checkIsZKTest()
    _adminZkClient
  }

  var _zookeeper: EmbeddedZookeeper = null

  def zookeeper: EmbeddedZookeeper = {
    checkIsZKTest()
    _zookeeper
  }

  def zkPort: Int = {
    checkIsZKTest()
    zookeeper.port
  }

  def zkConnect: String = {
    if (_isKRaftTest) {
      null
    } else {
      s"127.0.0.1:$zkPort"
    }
  }

  def checkIsKRaftTest(): Unit = {
    if (!_isKRaftTest) {
      throw new RuntimeException("This function can't be accessed when running the test " +
        "in ZooKeeper mode. KRaft mode is required.")
    }
  }

  protected def controllerListenerSecurityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

  private var _metadataDir: File = null

  private var _controllerServer: ControllerServer = null

  def controllerServer: ControllerServer = {
    checkIsKRaftTest()
    _controllerServer
  }

  val _controllerQuorumVotersFuture = new CompletableFuture[util.Map[Integer, AddressSpec]]

  private var _raftManager: KafkaRaftManager[ApiMessageAndVersion] = null

  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = {
    checkIsKRaftTest()
    _raftManager
  }

  private var _kraftClusterId: String = null

  @BeforeEach
  def setUp(testInfo: TestInfo): Unit = {
    if (TestInfoUtils.isKRaft(testInfo)) {
      info(s"Running KRAFT test ${testInfo.getTestMethod}")
      _isKRaftTest = true
      setUpKRaft(testInfo)
    } else {
      info(s"Running ZK test ${testInfo.getTestMethod}")
      _isKRaftTest = false
      setUpZk()
    }
  }

  def createAndStartBroker(config: KafkaConfig,
                           time: Time = Time.SYSTEM): BrokerServer = {
    checkIsKRaftTest()
    System.out.println("config = " + config.originals)
    val broker = new BrokerServer(config = config,
      metaProps = new MetaProperties(_kraftClusterId, config.nodeId),
      raftManager = _raftManager,
      time = time,
      metrics = new Metrics(),
      threadNamePrefix = Some("Broker%02d_".format(config.nodeId)),
      initialOfflineDirs = Seq(),
      controllerQuorumVotersFuture = _controllerQuorumVotersFuture,
      supportedFeatures = Collections.emptyMap())
    broker.startup()
    broker
  }

  private def formatDirectories(directories: immutable.Seq[String],
                                metaProperties: MetaProperties): Unit = {
    val stream = new ByteArrayOutputStream()
    var out: PrintStream = null
    try {
      out = new PrintStream(stream)
      if (StorageTool.formatCommand(out, directories, metaProperties, false) != 0) {
        throw new RuntimeException(out.toString())
      }
      debug(s"Formatted storage directory(ies) ${directories}")
    } finally {
      if (out != null) out.close()
      stream.close()
    }
  }

  private def setUpKRaft(testInfo: TestInfo): Unit = {
    _kraftClusterId = Uuid.randomUuid().toString
    _metadataDir = TestUtils.tempDir()
    val metaProperties = new MetaProperties(_kraftClusterId, 0)
    formatDirectories(immutable.Seq(_metadataDir.getAbsolutePath()), metaProperties)
    val controllerMetrics = new Metrics()
    val props = new util.HashMap[String, String]()
    props.put(KafkaConfig.ProcessRolesProp, "controller")
    props.put(KafkaConfig.NodeIdProp, "1000")
    props.put(KafkaConfig.MetadataLogDirProp, _metadataDir.getAbsolutePath())
    val proto = controllerListenerSecurityProtocol.toString()
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"${proto}:${proto}")
    props.put(KafkaConfig.ListenersProp, s"${proto}://localhost:0")
    props.put(KafkaConfig.ControllerListenerNamesProp, proto)
    props.put(KafkaConfig.QuorumVotersProp, "1000@localhost:0")
    val config = new KafkaConfig(props)
    val threadNamePrefix = "Controller_" + testInfo.getDisplayName
    _raftManager = new KafkaRaftManager(
      metaProperties = metaProperties,
      config = config,
      recordSerde = MetadataRecordSerde.INSTANCE,
      topicPartition = new TopicPartition(KafkaRaftServer.MetadataTopic, 0),
      topicId = KafkaRaftServer.MetadataTopicId,
      time = Time.SYSTEM,
      metrics = controllerMetrics,
      threadNamePrefixOpt = Option(threadNamePrefix),
      controllerQuorumVotersFuture = _controllerQuorumVotersFuture)
    _controllerServer = new ControllerServer(
      metaProperties = metaProperties,
      config = config,
      raftManager = _raftManager,
      time = Time.SYSTEM,
      metrics = controllerMetrics,
      threadNamePrefix = Option(threadNamePrefix),
      controllerQuorumVotersFuture = _controllerQuorumVotersFuture)

    _controllerServer.socketServerFirstBoundPortFuture.whenComplete((port, e) => {
      if (e != null) {
        error("Error completing controller socket server future", e)
        _controllerQuorumVotersFuture.completeExceptionally(e)
      } else {
        _controllerQuorumVotersFuture.complete(Collections.singletonMap(1000,
          new InetAddressSpec(new InetSocketAddress("localhost", port))));
      }
    });
    _controllerServer.startup()
    _raftManager.startup()
    _controllerServer.startup()
  }

  private def setUpZk(): Unit = {
    _zookeeper = new EmbeddedZookeeper()
    _zkClient = KafkaZkClient(zkConnect, zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled), zkSessionTimeout,
      zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM, name = "ZooKeeperTestHarness", new ZKClientConfig)
    _adminZkClient = new AdminZkClient(_zkClient)
  }

  @AfterEach
  def tearDown(): Unit = {
    shutdownZooKeeper()
    shutdownControllerQuorum()
    Configuration.setConfiguration(null)
  }

  def shutdownZooKeeper(): Unit = {
    if (_zkClient != null)
      _zkClient.close()
    if (_zookeeper != null)
      CoreUtils.swallow(_zookeeper.shutdown(), this)
  }

  def shutdownControllerQuorum(): Unit = {
    if (_raftManager != null) {
      try {
        _raftManager.shutdown()
      } catch {
        case e: Throwable => error("Error shutting down KafkaRaftManager", e)
      }
    }
    if (_controllerServer != null) {
      try {
        _controllerServer.shutdown()
      } catch {
        case e: Throwable => error("Error shutting down ControllerServer", e)
      }
    }
  }

  // Trigger session expiry by reusing the session id in another client
  def createZooKeeperClientToTriggerSessionExpiry(zooKeeper: ZooKeeper): ZooKeeper = {
    val dummyWatcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    }
    val anotherZkClient = new ZooKeeper(zkConnect, 1000, dummyWatcher,
      zooKeeper.getSessionId,
      zooKeeper.getSessionPasswd)
    assertNull(anotherZkClient.exists("/nonexistent", false)) // Make sure new client works
    anotherZkClient
  }
}

object ZooKeeperTestHarness {
  val ZkClientEventThreadSuffix = "-EventThread"

  /**
   * Verify that a previous test that doesn't use ZooKeeperTestHarness hasn't left behind an unexpected thread.
   * This assumes that brokers, ZooKeeper clients, producers and consumers are not created in another @BeforeClass,
   * which is true for core tests where this harness is used.
   */
  @BeforeAll
  def setUpClass(): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@BeforeAll")
  }

  /**
   * Verify that tests from the current test class using ZooKeeperTestHarness haven't left behind an unexpected thread
   */
  @AfterAll
  def tearDownClass(): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@AfterAll")
  }

}
