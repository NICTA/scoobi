package com.nicta.scoobi
package impl
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
  def getFileStatus(path: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit conf: Configuration): Seq[FileStatus] =
    tryOrElse {
      ?(FileSystem.get(path.toUri, conf).globStatus(new Path(path, "*"), pathFilter)) match {
        case None    => Seq.empty
        case Some(s) => s.toSeq
      }
    }(Seq.empty)

  /** Only get one file per dir. This helps when checking correctness of input data by reducing
    *  the number of files to check. We don't want to check every file as its expensive */
  def getSingleFilePerDir(path: Path)(implicit conf: Configuration): Set[Path] = {
    getSingleFilePerDir(getFileStatus(path))
  }

  def getSingleFilePerDir(stats: Seq[FileStatus])(implicit conf: Configuration): Set[Path] = {
    stats.groupBy(_.getPath.getParent).flatMap(_._2.filterNot(_.isDirectory).headOption.map(_.getPath)).toSet
  }

  def deletePath(p: Path)(implicit conf: Configuration) = FileSystem.get(conf).delete(p, true)

  /** Determine the byte size of data specified by a path. */
  def pathSize(p: Path)(implicit conf: Configuration): Long = {
    val fs = FileSystem.get(p.toUri, conf)
    tryOrElse {
      ?(fs.globStatus(p)).getOrElse(Array()).map { stat =>
        fs.getContentSummary(stat.getPath).getLength
      }.sum
    }(0)
  }

  /** Provide a nicely formatted string for a byte size. */
  def sizeString(bytes: Long): String = {
    val gigabytes = (bytes / (1000 * 1000 * 1000).toDouble).toString
    gigabytes + "GB"
  }
}
