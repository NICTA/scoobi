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
package impl
package io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import impl.control.Exceptions._
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import scalaz.std.anyVal._
import java.net.URI
import com.nicta.scoobi.impl.util.Compatibility
import org.apache.hadoop.security.UserGroupInformation
import java.io.IOException
import org.apache.commons.logging.LogFactory

/**
 * A set of helper functions for implementing DataSources and DataSinks
 */
trait Files {
  private implicit lazy val logger = LogFactory.getLog("scoobi.Files")

  /**
   * @return the file system for a given path and configuration
   */
  def fileSystem(path: Path)(implicit configuration: Configuration) = FileSystem.get(path.toUri, configuration)

  /** @return a function moving a Path to a given directory */
  def moveTo(dir: String)(implicit configuration: Configuration): Path => Boolean = (path: Path) =>
    moveTo(new Path(dirPath(dir))).apply(path, new Path(path.getName))

  /** @return a function moving a Path to a given directory */
  def moveTo(dir: Path)(implicit configuration: Configuration): (Path, Path) => Boolean = (path: Path, newPath: Path) => {
    !pathExists(path) || {
      val from = fileSystem(path)
      val to   = fileSystem(dir)

      val destPath = to.makeQualified(new Path(dirPath(dir.toString) + newPath))
      if (!pathExists(destPath.getParent)) to.mkdirs(destPath.getParent)

      if (List("s3n", "s3").contains(Compatibility.getScheme(to).toLowerCase))
        // s3 has special cases (can't rename, can't copy/rename dir simultaneously, ...)
        moveToS3(from, to, path, destPath)

      else if (sameFileSystem(from, to))
        (path == destPath) || // same files
        (Compatibility.isDirectory(from, path) &&
         Compatibility.isDirectory(to, destPath) &&
          path.toUri.getPath.startsWith(destPath.toUri.getPath)) || // nested directories
        {
          logger.debug(s"renaming $path to $destPath")
          tryOk {
            Compatibility.rename(path, destPath)
          }
        }
      else {
        logger.debug(s"copying $path to $destPath")
        FileUtil.copy(from, path, to, destPath,
          true /* deleteSource */, true /* overwrite */, configuration)
      }
    }
  }

  /**
   * @return true if copy to S3 is successful. S3 is a special cases
   *         because it can't rename, can't copy/rename dir simultaneously, ...
   */
  def moveToS3(from: FileSystem, to: FileSystem,
               path: Path, destPath: Path)(implicit configuration: Configuration) = {
    if (Compatibility.isDirectory(from, path)) {
      // copying from a dir/ to s3 requires copying individual dir/* files
      val sourceFiles = FileSystem.get (path.toUri, configuration).listStatus(path)
        .toSeq.map (_.getPath).toList
      sourceFiles.forall { sourceFile =>
        // TODO: do parallel S3 copy to speed up process
        logger.debug(s"individually copying $sourceFile to $destPath (S3)")
        FileUtil.copy(from, sourceFile, to, destPath,
          true /* deleteSource */, true /* overwrite */, configuration)
      }
    } else {
      // copying from one dir to S3
      logger.debug(s"copying $path to $destPath (S3)")
      FileUtil.copy(from, path, to, destPath,
        true /* deleteSource */, true /* overwrite */, configuration)
    }
  }

  /** @return a function copying a Path to a given directory */
  def copyTo(dir: String)(implicit configuration: Configuration): Path => Boolean = copyTo(new Path(dir))

  /** @return a function copying a Path to a given directory */
  def copyTo(dir: Path)(implicit configuration: Configuration): Path => Boolean = (f: Path) =>
    FileUtil.copy(fileSystem(f), f, fileSystem(dir), dir, false, configuration)

  /** @return the path with a trailing slash */
  def dirPath(s: String) = if (s endsWith "/") s else s+"/"

  /** @return the file status of a file */
  def fileStatus(path: Path)(implicit configuration: Configuration) = fileSystem(path).getFileStatus(path)

  /** @return true if the 2 fileSystems are the same */
  def sameFileSystem(from: FileSystem, to: FileSystem): Boolean = {
    def equalIgnoreCase(from: String, to: String) = (from == null && to == null) || from.equalsIgnoreCase(to)
    equalIgnoreCase(from.getUri.getHost, to.getUri.getHost) && equalIgnoreCase(Compatibility.getScheme(from), Compatibility.getScheme(to))
  }

  /** @return true if the file is a directory */
  def isDirectory(fileStatus: FileStatus): Boolean = Compatibility.isDirectory(fileStatus)
  /** @return true if the path is a directory */
  def isDirectory(path: Path)(implicit configuration: Configuration): Boolean = isDirectory(fileStatus(path))

  /** check if a READ or WRITE action can be done on a given file based on the current user and on the file permissions */
  def checkFilePermissions(path: Path, action: FsAction)(implicit configuration: Configuration) = {
    val existingParent = getExistingParent(path)
    val existingParentFileStatus =  fileStatus(existingParent)
    val permission = getFilePermission(existingParent)

    val (group, owner, user) = (existingParentFileStatus.getGroup, existingParentFileStatus.getOwner, UserGroupInformation.getCurrentUser)
    val ownerIsUser = owner == user.getShortUserName || owner == user.getUserName

    if (ownerIsUser && !permission.getUserAction.implies(action) &&
      user.getGroupNames.contains(group) && !permission.getGroupAction.implies(action) &&
      !permission.getOtherAction.implies(action))
      throw new IOException(s"You do not have $action permission on the path: $permission $path")
  }

  /** Determine whether a path exists or not. */
  def pathExists(p: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit configuration: Configuration): Boolean = tryOrElse {
    val fs = fileSystem(p)
    (fs.isFile(p) && fs.exists(p)) || globStatus(p, pathFilter).nonEmpty
  }(false)

  /** Determine whether a path exists or not. */
  def pathExists(p: String)(implicit configuration: Configuration): Boolean = pathExists(new Path(p))

    /** Get a Set of FileStatus objects for a given Path specified as a glob */
  def globStatus(path: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit configuration: Configuration): Seq[FileStatus] =
    tryOrElse {
      val pathToSearch =
        if (path.toString.contains("*") || !fileSystem(path).isDirectory(path)) path
        else                                                                    new Path(path, "*")

      Option(fileSystem(path).globStatus(pathToSearch, pathFilter)).map(_.toSeq).getOrElse(Seq())
    }(Seq())

  @deprecated(message = "use globStatus instead", since = "0.7.3")
  def fileStatus(path: Path, pathFilter: PathFilter = hiddenFilePathFilter)(implicit configuration: Configuration) = globStatus(path, pathFilter)

  /** @return a PathFilter for hidden paths */
  private val hiddenFilePathFilter = new PathFilter {
    def accept(p: Path): Boolean = !p.getName.startsWith("_") && !p.getName.startsWith(".")
  }

  /**
   * Only get one file per dir. This helps when checking correctness of input data by reducing
   * the number of files to check. We don't want to check every file as its expensive
   */
  def getSingleFilePerDir(path: Path)(implicit configuration: Configuration): Set[Path] = {
    getSingleFilePerDir(globStatus(path))
  }

  def getSingleFilePerDir(stats: Seq[FileStatus])(implicit configuration: Configuration): Set[Path] = {
    stats.groupBy(_.getPath.getParent).flatMap(_._2.filterNot(isDirectory).headOption.map(_.getPath)).toSet
  }

  /** delete the file found at a specific path */
  def deletePath(p: Path)(implicit configuration: Configuration) = fileSystem(p).delete(p, true)

  /** Determine the byte size of data specified by a path. */
  def pathSize(p: Path)(implicit configuration: Configuration): Long = {
    val fs = fileSystem(p)
    tryOrZero {
      fs.globStatus(p).map(stat => fs.getContentSummary(stat.getPath).getLength).sum
    }
  }

  /** Provide a nicely formatted string for a byte size. */
  def sizeString(bytes: Long): String = {
    val idx = (math.log(bytes) / math.log(1024)).toInt
    Seq("bytes", "KiB", "MiB", "GiB", "TiB", "PiB").lift(idx).map { unit =>
      ("%.2f " format (bytes / math.pow(1024, idx))) + unit
    }.getOrElse(bytes + " bytes?!")
  }

  /** @return the permission for a given path */
  def getFilePermission(path:Path)(implicit configuration: Configuration): FsPermission =
    FileSystem.get(path.toUri, configuration).getFileStatus(path).getPermission

  /** @return the first existing parent for this path */
  def getExistingParent(path:Path)(implicit configuration: Configuration): Path = {
    val fs = FileSystem.get(path.toUri, configuration)
    if (fs.exists(path.getParent)) path.getParent
    else                           getExistingParent(path.getParent)
  }
}

object Files extends Files

@deprecated(message = "use Files instead", since = "0.7.3")
object Helper extends Files
