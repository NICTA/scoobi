package com.nicta.scoobi.testing

import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * This trait helps with the creation of temporary files and directories
 */
trait TempFiles {

  /** create a temporary file */
  def createTempFile(prefix: String) = File.createTempFile(prefix, "")

  /**
   * create a temporary directory by creating a temporary file and using that name to create a directory
   * this functionality should be reimplemented with Files.createTempDirectory once Java 7 becomes the default jvm for
   * Scoobi
   */
  def createTempDir(prefix: String) = {
    val d = File.createTempFile(prefix, "")
    d.delete
    d.mkdirs
    d
  }

  /**
   * delete a file that is supposed to be either local or remote
   */
  def deleteFile(p: String)(implicit fs: FileSystem) {
    fs.delete(new Path(p), true)
  }

  /**
   * delete a file that is supposed to be either local or remote
   */
  def deleteFile(f: File)(implicit fs: FileSystem) {
    deleteFile(f.getPath)
  }

  /**
   * @return the path of a file, either as an absolute path if it is a local file, or just its name if it is fetched with
   *         hdfs
   */
  def path(file: File, isRemote: Boolean) = {
    if (isRemote) file.getName
    else          file.getPath
  }

  /**
   * @return the path of a file, either as an absolute path if it is a local file, or just its name if it is fetched with
   *         hdfs
   */
  def getFiles(dir: File, isRemote: Boolean)(implicit fs: FileSystem) = {
    if (isRemote) {
      listPaths(path(dir, true)).foreach { p =>
        fs.copyToLocalFile(p, new Path(dir.getPath))
      }
    }
    listFiles(dir.getPath).filterNot(_.getPath contains "crc")
  }

  /** @return a list of elements which is empty if the input array is null or empty */
  private def safeSeq[A](array: Array[A]) = Option(array).map(_.toSeq).getOrElse(Seq[A]())

  /** @return a list of remote paths which is empty if there are no paths */
  private def listPaths(dir: String)(implicit fs: FileSystem) =
    safeSeq(fs.listStatus(new Path(dir))).map(f => f.getPath)

  /** @return a list of local files which is empty if there are no files */
  private def listFiles(dir: String)(implicit fs: FileSystem) =
    safeSeq(new File(dir).listFiles)

  /**
   * write lines to a file and return its local or remote path
   */
  def writeLines(file: File, lines: Seq[String], isRemote: Boolean)(implicit fs: FileSystem): String = {
    printToFile(file, isRemote)(p => lines foreach p.println)
    path(file, isRemote)
  }

  /**
   * write to a file and copy it to the cluster if we're executing a distributed job
   */
  private def printToFile(f: java.io.File, isRemote: Boolean)(op: java.io.PrintWriter => Unit)(implicit fs: FileSystem) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
    if (isRemote)
      fs.copyFromLocalFile(new Path(f.getPath), new Path(f.getName))
  }

}

object TempFiles extends TempFiles

