/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package core

import java.util.{Calendar, UUID}
import org.apache.hadoop.fs.{FileSystem, Path}

/** store the output name of a Sink as a checkpoint */
case class Checkpoint(name: String, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default) {
  def setup(outputPath: Path, sc: ScoobiConfiguration) = expiryPolicy.setup(outputPath, sc)
  def isExpired(outputPath: Path, sc: ScoobiConfiguration) = expiryPolicy.isExpired(outputPath, sc)
}

/**
 * Define the expiry policy for checkpoint files
 *
 * You can define
 *
 *  - the expiry time: how long a checkpoint file is valid (long value representing milliseconds)
 *  - the versioning strategy: what you do with an expired file (delete it, rename it,...)
 *
 */
case class ExpiryPolicy(expiryTime: Long = -1, versioning: (Path, ScoobiConfiguration) => Unit = ExpiryPolicy.deleteOldFile) {

  /**
   * @return true if an expiry time is set and if there exists a checkpoint file that's older than now - expiryTime
   */
  def isExpired(outputPath: Path, sc: ScoobiConfiguration) = {
    val fs = sc.fileSystem
    (expiryTime <= 0) || (
    (expiryTime > 0)        &&
    (fs.exists(outputPath)) &&
    ((fs.getFileStatus(outputPath).getModificationTime + expiryTime) > Calendar.getInstance.getTimeInMillis))
  }

  /**
   * Apply the versioning policy before trying to use the checkpoint file again
   */
  def setup(outputPath: Path, sc: ScoobiConfiguration) = {
    if (isExpired(outputPath, sc)) versioning(outputPath, sc)
  }
}

object ExpiryPolicy {
  type VersioningPolicy = (Path, ScoobiConfiguration) => Unit

  /** delete the previous checkpoint file */
  val deleteOldFile: VersioningPolicy = (p: Path, sc: ScoobiConfiguration) => {
    sc.fileSystem.delete(p, true)
  }

  /** rename the previous checkpoint file with an increasing version number */
  val incrementCounterFile: VersioningPolicy = (p: Path, sc: ScoobiConfiguration) => {
    val fs = sc.fileSystem
    val newIndex = lastIndex(p, sc).map(_ + 1).getOrElse(1)
    fs.rename(p, new Path(p.getParent, s"$p-$newIndex"))
  }

  /** rename the previous checkpoint file with an increasing version number, and remove the n oldest files */
  def incrementCounterAndRemoveLast(n: Int): VersioningPolicy = (p: Path, sc: ScoobiConfiguration) => {
    incrementCounterFile(p, sc)
    val fs = sc.fileSystem
    oldCheckpointFiles(p, sc).take(n).foreach(f => fs.delete(f.getPath, true))
  }

  /**
   * @return the old checkpoint files from the oldest to the newest
   */
  private def oldCheckpointFiles(p: Path, sc: ScoobiConfiguration) = {
    val fs = sc.fileSystem
    fs.listStatus(p.getParent).filter(_.getPath.getName.matches(p.getName+"\\-\\d+")).sortBy(_.getModificationTime)
  }

  /**
   * @return the maximum value of the index used to version checkpoint files
   */
  private def lastIndex(p: Path, sc: ScoobiConfiguration) =
    oldCheckpointFiles(p, sc).flatMap(_.getPath.getName.split("\\-").lastOption).map(_.toInt).sorted.lastOption

  val default = ExpiryPolicy()
}

object Checkpoint {
  def create(path: Option[String], expiryPolicy: ExpiryPolicy, doIt: Boolean)(implicit sc: ScoobiConfiguration) =
    if (doIt) Some(Checkpoint(path.map(_.toString).getOrElse(UUID.randomUUID().toString))) else None
}