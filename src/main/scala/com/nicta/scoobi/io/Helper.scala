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
package io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter, FileSystem, FileStatus}

import Option.{apply => ?}
import impl.control.Exceptions._

/** A set of helper functions for implementing DataSources and DataSinks. */
object Helper {

  private val hiddenFilePathFilter = new PathFilter {
    def accept(p: Path): Boolean = !p.getName.startsWith("_") && !p.getName.startsWith(".")
  }

  /** Determine whether a path exists or not. */
  def pathExists(p: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit conf: Configuration): Boolean = tryOrElse {
    getFileStatus(p, pathFilter).size > 0
  }(false)

  /** Get a Set of FileStatus objects for a given Path. */
  def getFileStatus(path: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit conf: Configuration): Seq[FileStatus] = {
    ?(FileSystem.get(path.toUri, conf).globStatus(new Path(path, "*"), pathFilter)) match {
      case None => Seq()
      case Some(s) => s.toSeq
    }
  }

  /** Only get one file per dir. This helps when checking correctness of input data by reducing
   *  the number of files to check. We don't want to check every file as its expensive */
  def getSingleFilePerDir(path: Path)(implicit conf: Configuration): Set[Path] = {
    getSingleFilePerDir(getFileStatus(path))
  }

  def getSingleFilePerDir(stats: Seq[FileStatus])(implicit conf: Configuration): Set[Path] = {
    stats.groupBy(_.getPath.getParent).flatMap(_._2.filterNot(_.isDir).headOption.map(_.getPath)).toSet
  }

  def deletePath(p: Path)(implicit conf: Configuration) = FileSystem.get(conf).delete(p, true)

  /** Determine the byte size of data specified by a path. */
  def pathSize(p: Path)(implicit conf: Configuration): Long = {
    val fs = FileSystem.get(p.toUri, conf)
    Option(fs.globStatus(p)).getOrElse(Array()).map { stat =>
      fs.getContentSummary(stat.getPath).getLength
    }.sum
  }

  /** Provide a nicely formatted string for a byte size. */
  def sizeString(bytes: Long): String = {
    val gigabytes = (bytes / (1000 * 1000 * 1000).toDouble).toString
    gigabytes + "GB"
  }
}
