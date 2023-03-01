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

package kafka.server.metadata

import java.util.{OptionalInt, Properties}
import java.util.concurrent.atomic.AtomicLong
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.{KafkaConfig, ReplicaManager, RequestLocal}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.image.loader.{LogDeltaManifest, SnapshotManifest}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta, TopicsImage}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.fault.FaultHandler

import scala.collection.mutable
import scala.jdk.CollectionConverters._


object BrokerMetadataPublisher extends Logging {
  /**
   * Given a topic name, find out if it changed. Note: if a topic named X was deleted and
   * then re-created, this method will return just the re-creation. The deletion will show
   * up in deletedTopicIds and must be handled separately.
   *
   * @param topicName   The topic name.
   * @param newImage    The new metadata image.
   * @param delta       The metadata delta to search.
   *
   * @return            The delta, or None if appropriate.
   */
  def getTopicDelta(topicName: String,
                    newImage: MetadataImage,
                    delta: MetadataDelta): Option[TopicDelta] = {
    Option(newImage.topics().getTopic(topicName)).flatMap {
      topicImage => Option(delta.topicsDelta()).flatMap {
        topicDelta => Option(topicDelta.changedTopic(topicImage.id()))
      }
    }
  }

  /**
   * Find logs which should not be on the current broker, according to the metadata image.
   *
   * @param brokerId        The ID of the current broker.
   * @param newTopicsImage  The new topics image after broker has been reloaded
   * @param logs            A collection of Log objects.
   *
   * @return          The topic partitions which are no longer needed on this broker.
   */
  def findStrayPartitions(brokerId: Int,
                          newTopicsImage: TopicsImage,
                          logs: Iterable[UnifiedLog]): Iterable[TopicPartition] = {
    logs.flatMap { log =>
      val topicId = log.topicId.getOrElse {
        throw new RuntimeException(s"The log dir $log does not have a topic ID, " +
          "which is not allowed when running in KRaft mode.")
      }

      val partitionId = log.topicPartition.partition()
      Option(newTopicsImage.getPartition(topicId, partitionId)) match {
        case Some(partition) =>
          if (!partition.replicas.contains(brokerId)) {
            info(s"Found stray log dir $log: the current replica assignment ${partition.replicas} " +
              s"does not contain the local brokerId $brokerId.")
            Some(log.topicPartition)
          } else {
            None
          }

        case None =>
          info(s"Found stray log dir $log: the topicId $topicId does not exist in the metadata image")
          Some(log.topicPartition)
      }
    }
  }
}

class BrokerMetadataPublisher(
  conf: KafkaConfig,
  metadataCache: Option[KRaftMetadataCache],
  logManager: Option[LogManager],
  replicaManager: Option[ReplicaManager],
  groupCoordinator: Option[GroupCoordinator],
  txnCoordinator: Option[TransactionCoordinator],
  clientQuotaMetadataManager: ClientQuotaMetadataManager,
  var dynamicConfigPublisher: DynamicConfigPublisher,
  private val _authorizer: Option[Authorizer],
  fatalFaultHandler: FaultHandler,
  metadataPublishingFaultHandler: FaultHandler
) extends MetadataPublisher with Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  logIdent = s"[BrokerMetadataPublisher id=${conf.nodeId}] "

  def this(
    conf: KafkaConfig,
    clientQuotaMetadataManager: ClientQuotaMetadataManager,
    dynamicConfigPublisher: DynamicConfigPublisher,
    fatalFaultHandler: FaultHandler,
    metadataPublishingFaultHandler: FaultHandler,
  ) = this(
    conf,
    None,
    None,
    None,
    None,
    None,
    clientQuotaMetadataManager,
    dynamicConfigPublisher,
    None,
    fatalFaultHandler,
    metadataPublishingFaultHandler)

  import BrokerMetadataPublisher._

  /**
   * The broker ID.
   */
  val brokerId: Int = conf.nodeId

  /**
   * True if this is the first time we have published metadata.
   */
  var _firstPublish = true

  /**
   * This is updated after all components (e.g. LogManager) has finished publishing the new metadata delta
   */
  val publishedOffsetAtomic = new AtomicLong(-1)

  override def publish(delta: MetadataDelta, newImage: MetadataImage): Unit = {
    val highestOffsetAndEpoch = newImage.highestOffsetAndEpoch()

    val deltaName = if (_firstPublish) {
      s"initial MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    } else {
      s"MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    }
    try {
      if (isTraceEnabled) {
        trace(s"Publishing delta $delta with highest offset $highestOffsetAndEpoch")
      }

      // Publish the new metadata image to the metadata cache.
      metadataCache.foreach(_.setImage(newImage))

      val metadataVersionLogMsg = s"metadata.version ${newImage.features().metadataVersion()}"

      if (_firstPublish) {
        info(s"Publishing initial metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")

        // If this is the first metadata update we are applying, initialize the managers
        // first (but after setting up the metadata cache).
        initializeManagers()
      } else if (isDebugEnabled) {
        debug(s"Publishing metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")
      }

      Option(delta.featuresDelta()).foreach { featuresDelta =>
        featuresDelta.metadataVersionChange().ifPresent{ metadataVersion =>
          info(s"Updating metadata.version to ${metadataVersion.featureLevel()} at offset $highestOffsetAndEpoch.")
        }
      }

      // Apply topic deltas.
      Option(delta.topicsDelta()).foreach { topicsDelta =>
        replicaManager.foreach(replicaManager =>
          try {
            // Notify the replica manager about changes to topics.
            replicaManager.applyDelta(topicsDelta, newImage)
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error applying topics " +
              s"delta in ${deltaName}", t)
          })
        groupCoordinator.foreach(groupCoordinator =>
          try {
            // Update the group coordinator of local changes
            updateCoordinator(newImage,
              delta,
              Topic.GROUP_METADATA_TOPIC_NAME,
              groupCoordinator.onElection,
              (partitionIndex, leaderEpochOpt) => groupCoordinator.onResignation(partitionIndex, toOptionalInt(leaderEpochOpt))
            )
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
              s"coordinator with local changes in ${deltaName}", t)
          })
        txnCoordinator.foreach(txnCoordinator =>
          try {
            // Update the transaction coordinator of local changes
            updateCoordinator(newImage,
              delta,
              Topic.TRANSACTION_STATE_TOPIC_NAME,
              txnCoordinator.onElection,
              txnCoordinator.onResignation)
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating txn " +
              s"coordinator with local changes in ${deltaName}", t)
          })
        groupCoordinator.foreach(groupCoordinator =>
          try {
            // Notify the group coordinator about deleted topics.
            val deletedTopicPartitions = new mutable.ArrayBuffer[TopicPartition]()
            topicsDelta.deletedTopicIds().forEach { id =>
              val topicImage = topicsDelta.image().getTopic(id)
              topicImage.partitions().keySet().forEach {
                id => deletedTopicPartitions += new TopicPartition(topicImage.name(), id)
              }
            }
            if (deletedTopicPartitions.nonEmpty) {
              groupCoordinator.onPartitionsDeleted(deletedTopicPartitions.asJava, RequestLocal.NoCaching.bufferSupplier)
            }
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
              s"coordinator with deleted partitions in ${deltaName}", t)
          })
      }

      // Apply configuration deltas.
      dynamicConfigPublisher.publish(delta, newImage)

      // Apply client quotas delta.
      try {
        Option(delta.clientQuotasDelta()).foreach { clientQuotasDelta =>
          clientQuotaMetadataManager.update(clientQuotasDelta)
        }
      } catch {
        case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating client " +
          s"quotas in ${deltaName}", t)
      }

      // Apply changes to ACLs. This needs to be handled carefully because while we are
      // applying these changes, the Authorizer is continuing to return authorization
      // results in other threads. We never want to expose an invalid state. For example,
      // if the user created a DENY ALL acl and then created an ALLOW ACL for topic foo,
      // we want to apply those changes in that order, not the reverse order! Otherwise
      // there could be a window during which incorrect authorization results are returned.
      Option(delta.aclsDelta()).foreach( aclsDelta =>
        _authorizer match {
          case Some(authorizer: ClusterMetadataAuthorizer) => if (aclsDelta.isSnapshotDelta) {
            try {
              // If the delta resulted from a snapshot load, we want to apply the new changes
              // all at once using ClusterMetadataAuthorizer#loadSnapshot. If this is the
              // first snapshot load, it will also complete the futures returned by
              // Authorizer#start (which we wait for before processing RPCs).
              authorizer.loadSnapshot(newImage.acls().acls())
            } catch {
              case t: Throwable => metadataPublishingFaultHandler.handleFault("Error loading " +
                s"authorizer snapshot in ${deltaName}", t)
            }
          } else {
            try {
              // Because the changes map is a LinkedHashMap, the deltas will be returned in
              // the order they were performed.
              aclsDelta.changes().entrySet().forEach(e =>
                if (e.getValue.isPresent) {
                  authorizer.addAcl(e.getKey, e.getValue.get())
                } else {
                  authorizer.removeAcl(e.getKey)
                })
            } catch {
              case t: Throwable => metadataPublishingFaultHandler.handleFault("Error loading " +
                s"authorizer changes in ${deltaName}", t)
            }
          }
          case _ => // No ClusterMetadataAuthorizer is configured. There is nothing to do.
        })

      if (_firstPublish && replicaManager.nonEmpty) {
        finishInitializingReplicaManager(newImage)
      }
      publishedOffsetAtomic.set(newImage.highestOffsetAndEpoch().offset)
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Uncaught exception while " +
        s"publishing broker metadata from ${deltaName}", t)
    } finally {
      _firstPublish = false
    }
  }

  private def toOptionalInt(option: Option[Int]): OptionalInt = {
    option match {
      case Some(leaderEpoch) => OptionalInt.of(leaderEpoch)
      case None => OptionalInt.empty
    }
  }

  override def publishedOffset: Long = publishedOffsetAtomic.get()

  def reloadUpdatedFilesWithoutConfigChange(props: Properties): Unit = {
    conf.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
  }

  /**
   * Update the coordinator of local replica changes: election and resignation.
   *
   * @param image latest metadata image
   * @param delta metadata delta from the previous image and the latest image
   * @param topicName name of the topic associated with the coordinator
   * @param election function to call on election; the first parameter is the partition id;
   *                 the second parameter is the leader epoch
   * @param resignation function to call on resignation; the first parameter is the partition id;
   *                    the second parameter is the leader epoch
   */
  def updateCoordinator(
    image: MetadataImage,
    delta: MetadataDelta,
    topicName: String,
    election: (Int, Int) => Unit,
    resignation: (Int, Option[Int]) => Unit
  ): Unit = {
    // Handle the case where the topic was deleted
    Option(delta.topicsDelta()).foreach { topicsDelta =>
      if (topicsDelta.topicWasDeleted(topicName)) {
        topicsDelta.image.getTopic(topicName).partitions.entrySet.forEach { entry =>
          if (entry.getValue.leader == brokerId) {
            resignation(entry.getKey, Some(entry.getValue.leaderEpoch))
          }
        }
      }
    }

    // Handle the case where the replica was reassigned, made a leader or made a follower
    getTopicDelta(topicName, image, delta).foreach { topicDelta =>
      val changes = topicDelta.localChanges(brokerId)

      changes.deletes.forEach { topicPartition =>
        resignation(topicPartition.partition, None)
      }
      changes.leaders.forEach { (topicPartition, partitionInfo) =>
        election(topicPartition.partition, partitionInfo.partition.leaderEpoch)
      }
      changes.followers.forEach { (topicPartition, partitionInfo) =>
        resignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
      }
    }
  }

  private def initializeManagers(): Unit = {
    try {
      // Start log manager, which will perform (potentially lengthy)
      // recovery-from-unclean-shutdown if required.
      logManager.foreach(_.startup(metadataCache.get.getAllTopics()))

      // Make the LogCleaner available for reconfiguration. We can't do this prior to this
      // point because LogManager#startup creates the LogCleaner object, if
      // log.cleaner.enable is true. TODO: improve this (see KAFKA-13610)
      logManager.foreach(logManager => Option(logManager.cleaner).foreach(conf.dynamicConfig.addBrokerReconfigurable))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting LogManager", t)
    }
    try {
      // Start the replica manager.
      replicaManager.foreach(_.startup())
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting ReplicaManager", t)
    }
    try {
      // Start the group coordinator.
      groupCoordinator.foreach(_.startup(() => metadataCache.get.numPartitions(Topic.GROUP_METADATA_TOPIC_NAME)
        .getOrElse(conf.offsetsTopicPartitions)))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting GroupCoordinator", t)
    }
    try {
      // Start the transaction coordinator.
      txnCoordinator.foreach(_.startup(() => metadataCache.get.numPartitions(
        Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(conf.transactionTopicPartitions)))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting TransactionCoordinator", t)
    }
  }

  private def finishInitializingReplicaManager(newImage: MetadataImage): Unit = {
    try {
      // Delete log directories which we're not supposed to have, according to the
      // latest metadata. This is only necessary to do when we're first starting up. If
      // we have to load a snapshot later, these topics will appear in deletedTopicIds.
      val strayPartitions = findStrayPartitions(brokerId, newImage.topics, logManager.get.allLogs)
      if (strayPartitions.nonEmpty) {
        replicaManager.get.deleteStrayReplicas(strayPartitions)
      }
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Error deleting stray " +
        "partitions during startup", t)
    }
    try {
      // Make sure that the high water mark checkpoint thread is running for the replica
      // manager.
      replicaManager.get.startHighWatermarkCheckPointThread()
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Error starting high " +
        "watermark checkpoint thread during startup", t)
    }
}

  /**
   * Returns the name of this publisher.
   *
   * @return The publisher name.
   */
  override def name(): String = s"BrokerMetadataPublisher ${dynamicConfigPublisher.nodeType} id=${conf.nodeId}"

  /**
   * Publish a new cluster metadata snapshot that we loaded.
   *
   * @param delta    The delta between the previous state and the new one.
   * @param newImage The complete new state.
   * @param manifest The contents of what was published.
   */
  override def publishSnapshot(delta: MetadataDelta, newImage: MetadataImage, manifest: SnapshotManifest): Unit = {
    publish(delta, newImage)
  }

  /**
   * Publish a change to the cluster metadata.
   *
   * @param delta    The delta between the previous state and the new one.
   * @param newImage The complete new state.
   * @param manifest The contents of what was published.
   */
  override def publishLogDelta(delta: MetadataDelta, newImage: MetadataImage, manifest: LogDeltaManifest): Unit = {
    publish(delta, newImage)
  }

  /**
   * Close this metadata publisher.
   */
  override def close(): Unit = {
    // nothing to close
  }
}
