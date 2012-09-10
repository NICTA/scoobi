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
package testing

import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}
import sys.process._

import impl.control.Exceptions._

/**
 * This trait helps with the creation of temporary files and directories
 */
trait TempFiles {

  /** create a temporary file */
  def createTempFile(prefix: String): File = createTempFile(prefix, "")

  /** create a temporary file */
  def createTempFile(prefix: String, suffix: String): File = File.createTempFile(prefix, suffix)
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
  def deleteFile(p: String)(implicit fs: FileSystem) = {
    fs.delete(new Path(p), true)
  }

  /**
   * delete a file that is supposed to be either local or remote
   */
  def deleteFile(f: File, isRemote: Boolean = false)(implicit fs: FileSystem) {
    val result = deleteFile(path(f, isRemote))
    if (!result && !isRemote) ("rm -rf "+f.getPath).!
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
   * @return all the files in a given directory, bringing them back from the cluster if necessary
   */
  def getFiles(dir: File, isRemote: Boolean)(implicit fs: FileSystem) = {
    if (isRemote) {
      listPaths(path(dir, true)).foreach { p =>
        fs.copyToLocalFile(p, new Path(relativePath(dir, p.toUri.getPath)))
      }
    }
    listFiles(dir.getPath).filterNot(_.getPath contains "crc")
  }

  /**
   * @return a path starting with 'dir' path and ending with the part in 'path' that comes after 'dir' path:
   *         dir      = /var/temp/d1
   *         path     = /user/me/temp/d1/1/hello.txt
   *         relative = /var/temp/d1/1/hello.txt
   */
  def relativePath(dir: File, path: String): String = dir.getPath+path.substring(path.indexOf(dir.getName) + dir.getName.size)

  /** @return a list of elements which is empty if the input array is null or empty */
  private def safeSeq[A](array: Array[A]) = Option(array).map(_.toSeq).getOrElse(Seq[A]())

  /** @return a list of remote paths which is empty if there are no paths */
  private def listPaths(dir: String)(implicit fs: FileSystem): Seq[Path] = tryOrElse {
    safeSeq(fs.listStatus(new Path(dir))).flatMap {
      case f if fs.isFile(f.getPath) => Seq(f.getPath)
      case d                         => listPaths(d.getPath.toUri.getPath)
    }
  }(Seq.empty)

  /** @return a list of local files (recursively) which is empty if there are no files */
  private def listFiles(dir: String)(implicit fs: FileSystem): Seq[File] = {
   if (new File(dir).isDirectory) {
     val files = safeSeq(new File(dir).listFiles)
     files.filterNot(_.isDirectory) ++ files.flatMap(f => listFiles(f.getPath))
   } else Seq[File]()
  }


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
  private def printToFile(f: java.io.File, isRemote: Boolean)(op: java.io.PrintWriter => scala.Unit)(implicit fs: FileSystem) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
    if (isRemote)
      fs.copyFromLocalFile(new Path(f.getPath), new Path(f.getName))
  }

}

object TempFiles extends TempFiles
