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

import java.io.File
import org.apache.hadoop.fs.{FileUtil, LocalFileSystem, Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache
import application._
import ScoobiConfiguration._
import org.apache.commons.logging.LogFactory
import impl.monitor.Loggable._
/**
 *
 */
private [scoobi]
trait FileSystems {

  private implicit val logger = LogFactory.getLog("scoobi.FileSystems")

  /**
   * Upload additional local jars to a destination directory on the hdfs
   * @return the original sequence of files
   */
  def uploadNewJars(sourceFiles: Seq[File], dest: String)(implicit configuration: ScoobiConfiguration): Seq[File] = {
    DistributedCache.createSymlink(configuration)
    uploadNewFiles(sourceFiles, dest) { path =>
      DistributedCache.addFileToClassPath(path, configuration)
      path
    }
  }

  /**
   * Upload additional local files to a destination directory on the hdfs
   */
  def uploadNewFiles(sourceFiles: Seq[File], dest: String)
                    (onRemoteFiles: Path => Path = identity)(implicit configuration: ScoobiConfiguration): Seq[File] = {

    val uploaded = listPaths(dest)

    val newFiles = sourceFiles.filter(_.exists).filterNot(isOldFile(uploaded))
    logger.debugNot(newFiles.isEmpty, "uploading files\n"+newFiles.mkString("\n"))
    newFiles.map { file: File =>
      fileSystem.copyFromLocalFile(new Path(file.getPath), new Path(dest))
    }

    uploaded foreach onRemoteFiles
    sourceFiles
  }

  /** @return true if a file can be found in the existing files and if its size has not changed */
  def isOldFile(existingFiles: Seq[Path])(implicit configuration: ScoobiConfiguration) = (file: File) =>
    existingFiles.exists { (path:Path) =>
      path.getName.contains(file.getName) &&
      fileStatus(path).getLen == file.length
    }.debugNot("the file "+file.getName+" was not found on the cluster or has changed")

  /** @return a non-null sequence of files contained in a given directory */
  def listFiles(path: String): Seq[File] = Option(new File(path).listFiles).map(_.toSeq).getOrElse(Seq())

  /** @return a non-null sequence of file paths contained in a given directory */
  def listFilePaths(path: String): Seq[String] = listFiles(path).map(_.getPath)

  /** @return the list of files in a given directory on the file system */
  def listPaths(dest: String)(implicit configuration: ScoobiConfiguration): Seq[Path] = listPaths(new Path(dest))

  /** @return the list of files in a given directory on the file system */
  def listPaths(dest: Path)(implicit configuration: ScoobiConfiguration): Seq[Path] = {
    if (!fileSystem.exists(dest)) Seq()
    else                          fileSystem.listStatus(dest).map(_.getPath)
  }

  /**
   * create a directory if it doesn't exist already
   */
  def mkdir(dest: String)(implicit configuration: ScoobiConfiguration) {
    if (!fileSystem.exists(new Path(dest)))
      fileSystem.mkdirs(new Path(dest))
  }


  /**
   * delete all the files in a given directory on the file system
   */
  def deleteFiles(dest: String)(implicit configuration: ScoobiConfiguration) {
    val destPath = new Path(dest)
    if (fileSystem.exists(destPath)) fileSystem.delete(destPath, true)
  }

  /**
   * @return the file system for a given configuration
   */
  def fileSystem(implicit configuration: ScoobiConfiguration) = FileSystem.get(configuration)

  /**
   * @return true if the file system is local
   */
  private[scoobi]
  def isLocal(implicit configuration: ScoobiConfiguration) = fileSystem.isInstanceOf[LocalFileSystem]

  /** @return a function moving a Path to a given directory */
  def moveTo(dir: String)(implicit sc: ScoobiConfiguration): Path => Boolean = moveTo(new Path(dir))

  /** @return a function moving a Path to a given directory */
  def moveTo(dir: Path)(implicit sc: ScoobiConfiguration): Path => Boolean = (f: Path) =>
    fileSystem.rename(f, new Path(dir, f.getName))

  /** @return a function copying a Path to a given directory */
  def copyTo(dir: String)(implicit sc: ScoobiConfiguration): Path => Boolean = copyTo(new Path(dir))

  /** @return a function copying a Path to a given directory */
  def copyTo(dir: Path)(implicit sc: ScoobiConfiguration): Path => Boolean = (f: Path) =>
    FileUtil.copy(fileSystem, f, fileSystem, dir, false, sc)

  /** @return the path with a trailing slash */
  def dirPath(s: String) = if (s endsWith "/") s else s+"/"

  /** @return the file status of a file */
  def fileStatus(path: Path)(implicit sc: ScoobiConfiguration) = fileSystem.getFileStatus(path)
}

private [scoobi]
object FileSystems extends FileSystems
