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

import java.io.{IOException, File}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs._
import impl.util.Compatibility.hadoop2._
import com.nicta.scoobi.{impl, core}
import core._
import impl.ScoobiConfiguration._
import impl.monitor.Loggable._
import scala.collection.JavaConversions._
/**
 * Utility methods for copying files in the local / remote filesystem or across file systems
 */
private [scoobi]
trait FileSystems extends Files {

  private implicit val logger = LogFactory.getLog("scoobi.FileSystems")

  /**
   * Upload additional local jars to a destination directory on the hdfs
   * @return the original sequence of files
   */
  def uploadNewJars(sourceFiles: Seq[File], dest: String)(implicit configuration: ScoobiConfiguration): Seq[File] = {
    cache.createSymlink(configuration)
    uploadNewFiles(sourceFiles, dest)
  }

  /**
   * Upload additional local files to a destination directory on the hdfs
   */
  def uploadNewFiles(sourceFiles: Seq[File], dest: String,
                     onRemoteFiles: Path => Path = identity)(implicit configuration: ScoobiConfiguration): Seq[File] = {

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
  def isOldFile(existingFiles: Seq[Path])(implicit sc: ScoobiConfiguration) = (file: File) =>
    existingFiles.exists { (path:Path) =>
      path.getName.contains(file.getName) &&
      fileStatus(path)(sc.configuration).getLen == file.length
    }.debugNot("the file "+file.getName+" was not found on the cluster or has changed")

  /** @return a non-null sequence of files contained in a given directory */
  def listFiles(path: String): Seq[File] = Option(new File(path).listFiles).map(_.toSeq).getOrElse(Seq())

  /** @return a non-null sequence of file paths contained in a given directory */
  def listFilePaths(path: String): Seq[String] = listFiles(path).map(_.getPath)

  /** @return the list of files in a given directory on the file system */
  def listPaths(dest: String)(implicit configuration: ScoobiConfiguration): Seq[Path] = listPaths(new Path(dest))

  /** @return the recursive list of files in a given directory on the file system */
  def listPaths(dest: Path)(implicit configuration: ScoobiConfiguration): Seq[Path] = {
    if (!fileSystem.exists(dest)) Seq()
    else {
      val paths = fileSystem.listStatus(dest).toSeq.map(_.getPath)
      val (directories, files) = paths.partition(fileSystem.isDirectory)
      files ++ directories.flatMap(listPaths)
    }
  }

  /** @return the list of children files in a given directory on the file system  - do not recurse */
  def listDirectPaths(dest: Path)(implicit configuration: ScoobiConfiguration): Seq[Path] = {
    if (!fileSystem.exists(dest)) Seq()
    else fileSystem.listStatus(dest).toSeq.map(_.getPath)
  }

  /**
   * create a directory if it doesn't exist already
   */
  def mkdir(dest: Path)(implicit sc: ScoobiConfiguration) = {
    if (!exists(dest))
      FileSystem.get(dest.toUri, sc.configuration).mkdirs(dest)
    else true
  }

  /**
   * create a directory if it doesn't exist already
   */
  def mkdir(dest: String)(implicit sc: ScoobiConfiguration): Boolean = {
    mkdir(new Path(dest))
  }

  def exists(path: Path)(implicit sc: ScoobiConfiguration): Boolean =
    FileSystem.get(path.toUri, sc.configuration).exists(path)

  /** @return true if a Path exists with this name on the file system */
  def exists(path: String)(implicit configuration: ScoobiConfiguration): Boolean =
    exists(new Path(path))

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

}


private [scoobi]
object FileSystems extends FileSystems
