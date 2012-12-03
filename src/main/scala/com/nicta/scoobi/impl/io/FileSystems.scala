package com.nicta.scoobi
package impl
package io

import java.io.File
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileUtil, LocalFileSystem, Path, FileSystem}
import org.apache.hadoop.filecache.DistributedCache
import com.nicta.scoobi.{impl, core}
import core._
import impl.ScoobiConfigurationImpl._
import impl.monitor.Loggable._

/**
 * Utility methods for copying files in the local / remote filesystem or across file systems
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
    if (!exists(dest))
      fileSystem.mkdirs(new Path(dest))
  }

  /** @return true if a Path exists with this name on the file system */
  def exists(path: String)(implicit configuration: ScoobiConfiguration) =
    fileSystem.exists(new Path(path))

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
  def moveTo(dir: Path)(implicit sc: ScoobiConfiguration): Path => Boolean = (f: Path) => {
    if (fileSystem.exists(f)) fileSystem.rename(f, new Path(dir, f.getName))
    else                      true
  }

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
