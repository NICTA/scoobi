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
import com.nicta.scoobi.impl.io.FileSystems
import scala.concurrent.duration.{Duration, FiniteDuration}

/** store the output path of a Sink as a checkpoint */
case class Checkpoint(path: Path, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default) {
  /** @return the name for the file used as checkpoint */
  lazy val name = path.getName

  /**
   * @return the path for the file used as checkpoint as a string
   *
   * Important! This needs to be computed only once because it is used with an identity map in the filledSink attribute
   * to determine if a sink has been filled or not
   */
  lazy val pathAsString = path.toString

  /** setup the checkpoint by possibly deleting/versioning the previous checkpoint file */
  def setup(implicit sc: ScoobiConfiguration) = if (outputIsSuccess(path, sc)) expiryPolicy.setup(path, sc)

  /** @return true if this checkpoint can be used for a further job */
  def isValid(implicit sc: ScoobiConfiguration) = outputIsSuccess(path, sc) && !hasExpired

  /** @return true if the checkpoint has expired */
  def hasExpired(implicit sc: ScoobiConfiguration) = expiryPolicy.hasExpired(path, sc)
  /** @return true if the files of this checkpoint are the result of a successful job */
  def outputIsSuccess(outputPath: Path, sc: ScoobiConfiguration) =
    Option(sc.fileSystem.listStatus(outputPath)).exists(_.toList.exists(_.getPath.getName == "_SUCCESS"))
}

/**
 * Define the expiry policy for checkpoint files
 *
 * You can define
 *
 *  - the expiry time: how long a checkpoint file is valid (long value representing milliseconds)
 *  - the archiving strategy: what you do with an expired file (delete it, rename it,...)
 *
 */
case class ExpiryPolicy(expiryTime: FiniteDuration = Duration.Zero, archive: (Path, ScoobiConfiguration) => Unit = ExpiryPolicy.deleteOldFile) {

  /**
   * @return true if there is no checkpoint file or if an expiry time is set and if there exists a checkpoint file that's older than now - expiryTime
   */
  def hasExpired(outputPath: Path, sc: ScoobiConfiguration) = {
    val fs = sc.fileSystem
    expiryTime.toMillis <= 0 && !fs.exists(outputPath) ||
    expiryTime.toMillis > 0  &&
    fs.getFileStatus(outputPath).getModificationTime + expiryTime.toMillis < Calendar.getInstance.getTimeInMillis
  }

  /**
   * Apply the versioning policy before trying to use the checkpoint file again
   */
  def setup(outputPath: Path, sc: ScoobiConfiguration) = {
    if (hasExpired(outputPath, sc)) archive(outputPath, sc)
  }
}

object ExpiryPolicy {
  type ArchivingPolicy = (Path, ScoobiConfiguration) => Unit

  /** delete the previous checkpoint file */
  val deleteOldFile: ArchivingPolicy = (p: Path, sc: ScoobiConfiguration) => {
    sc.fileSystem.delete(p, true)
  }

  /** rename the previous checkpoint file with an increasing version number */
  val incrementCounterFile: ArchivingPolicy = (p: Path, sc: ScoobiConfiguration) => {
    val newIndex = lastIndex(p, sc).map(_ + 1).getOrElse(1)
    FileSystems.copyTo(new Path(p.getParent, s"$p-$newIndex"))(sc)(p)
  }

  /** rename the previous checkpoint file with an increasing version number, and remove the n oldest files */
  def incrementCounterAndRemoveLast(n: Int): ArchivingPolicy = (p: Path, sc: ScoobiConfiguration) => {
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
    if (doIt) Some(Checkpoint(new Path(path.getOrElse(UUID.randomUUID.toString)), expiryPolicy)) else None
}