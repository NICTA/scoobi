package com.nicta.scoobi
package impl
package io

import java.io.File
import org.apache.hadoop.fs.{FileUtil, LocalFileSystem, Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache

import com.nicta.scoobi.{impl, core}
import core._
import impl.ScoobiConfigurationImpl._

/**
 * Utility methods for copying files in the local / remote filesystem or across file systems
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

  /** @return the list of files in a given directory on the file system */
  def listFiles(dest: String)(implicit configuration: ScoobiConfiguration): Seq[Path] = listFiles(new Path(dest))

  /** @return the list of files in a given directory on the file system */
  def listFiles(dest: Path)(implicit configuration: ScoobiConfiguration): Seq[Path] = {
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
   * @return true if the file system is loacl
   */
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
}


private [scoobi]
object FileSystems extends FileSystems
