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

package kafka.server

import java.nio.file.{Files, Paths}

import org.apache.kafka.common.utils.Time
import org.slf4j.Logger

import scala.collection.JavaConverters._

/**
 * Retrieves Linux /proc/self/io metrics.
 */
class LinuxIoMetricsCollector(val procPath: String, val time: Time, val logger: Logger) {
  import LinuxIoMetricsCollector._
  var lastUpdateMs = -1L
  var cachedReadBytes = 0L
  var cachedWriteBytes = 0L

  def readBytes(): Long = this.synchronized {
    val curMs = time.milliseconds()
    if (curMs != lastUpdateMs) {
      updateValues(curMs)
    }
    cachedReadBytes
  }

  def writeBytes(): Long = this.synchronized {
    val curMs = time.milliseconds()
    if (curMs != lastUpdateMs) {
      updateValues(curMs)
    }
    cachedWriteBytes
  }

  /**
   * Read /proc/self/io.
   *
   * Generally, each line in this file contains a prefix followed by a colon and a number.
   *
   * For example, it might contain this:
   * rchar: 4052
   * wchar: 0
   * syscr: 13
   * syscw: 0
   * read_bytes: 0
   * write_bytes: 0
   * cancelled_write_bytes: 0
   */
  def updateValues(now: Long): Boolean = this.synchronized {
    try {
      var newReadBytes = 0L
      var newWriteBytes = 0L
      var newCancelledWriteBytes = 0L
      val lines = Files.readAllLines(Paths.get(procPath, "self", "io")).asScala
      lines.foreach(line => {
        if (line.startsWith(READ_BYTES_PREFIX)) {
          newReadBytes = line.substring(READ_BYTES_PREFIX.size).toLong
        } else if (line.startsWith(WRITE_BYTES_PREFIX)) {
          newWriteBytes = line.substring(WRITE_BYTES_PREFIX.size).toLong
        } else if (line.startsWith(CANCELLED_WRITE_BYTES_PREFIX)) {
          newCancelledWriteBytes = line.substring(CANCELLED_WRITE_BYTES_PREFIX.size).toLong
        }
      })
      cachedReadBytes = newReadBytes
      cachedWriteBytes = newWriteBytes - newCancelledWriteBytes
      lastUpdateMs = now
      true
    } catch {
      case t: Throwable => {
        logger.warn("LinuxIoMetricsCollector: unable to update metrics", t)
        false
      }
    }
  }

  def usable(): Boolean = {
    updateValues(time.milliseconds())
  }
}

object LinuxIoMetricsCollector {
  val READ_BYTES_PREFIX = "read_bytes: "
  val WRITE_BYTES_PREFIX = "write_bytes: "
  val CANCELLED_WRITE_BYTES_PREFIX = "cancelled_write_bytes: "
}
