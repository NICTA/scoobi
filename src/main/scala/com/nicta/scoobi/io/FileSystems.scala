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
import org.apache.hadoop.fs.{LocalFileSystem, Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache
import application._
import ScoobiConfiguration._
/**
 *
 */
private [scoobi]
trait FileSystems {

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

    val uploaded = listFiles(dest)

    val newFiles = sourceFiles.filterNot((f: File) => uploaded.map(_.getName).contains(f.getName))
    newFiles.map { file: File =>
      fileSystem.copyFromLocalFile(new Path(file.getPath), new Path(dest))
    }

    uploaded foreach onRemoteFiles
    sourceFiles
  }

  /**
   * @return the list of files in a given directory on the file system
   */
  def listFiles(dest: String)(implicit configuration: ScoobiConfiguration): Seq[Path] = {
    if (!fileSystem.exists(new Path(dest))) Seq()
    else                                    fileSystem.listStatus(new Path(dest)).map(_.getPath)
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
   * @return true if the file system is loacl
   */
  def isLocal(implicit configuration: ScoobiConfiguration) = fileSystem.isInstanceOf[LocalFileSystem]

}

private [scoobi]
object FileSystems extends FileSystems
