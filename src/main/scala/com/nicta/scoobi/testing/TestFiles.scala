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
import scala.io.Source
import org.apache.hadoop.fs.{Path, FileSystem}
import Scoobi._
import io.FileSystems

/**
 * This trait creates input and output files which are temporary
 * The files paths are registered in the configuration so that they can be deleted when the job has been executed
 */
trait TestFiles {

  def createTempFile(prefix: String, keep: Boolean)(implicit configuration: ScoobiConfiguration) =
    registerFile(moveToRemote(TempFiles.createTempFile(prefix+configuration.jobId), keep), keep)

  def createTempFile(prefix: String)(implicit configuration: ScoobiConfiguration) =
    registerFile(moveToRemote(TempFiles.createTempFile(prefix+configuration.jobId)))

  def createTempFile(prefix: String, suffix: String)(implicit configuration: ScoobiConfiguration) =
    registerFile(moveToRemote(TempFiles.createTempFile(prefix+configuration.jobId, suffix)))

  def createTempFile(prefix: String, suffix: String, keep: Boolean)(implicit configuration: ScoobiConfiguration) =
    registerFile(moveToRemote(TempFiles.createTempFile(prefix+configuration.jobId, suffix), keep), keep)

  def createTempDir(prefix: String)(implicit configuration: ScoobiConfiguration) =
    registerFile(TempFiles.createTempDir(prefix+configuration.jobId))

  def getFiles(dir: File)(implicit configuration: ScoobiConfiguration): Seq[File] =
    TempFiles.getFiles(dir, isRemote)(fs)

  def getFiles(dir: String)(implicit configuration: ScoobiConfiguration): Seq[File] =
    getFiles(new File(dir))

  def deleteFiles(implicit configuration: ScoobiConfiguration) {
    deleteFiles(configuration.get("scoobi.test.files", "").split(",").toSeq.filterNot(_.isEmpty).map(new File(_)))
  }

  def path(path: String)(implicit configuration: ScoobiConfiguration): String = TempFiles.path(new File(path), isRemote)
  def path(file: File)(implicit configuration: ScoobiConfiguration): String = TempFiles.path(file, isRemote)

  def isRemote(implicit configuration: ScoobiConfiguration) = configuration.isRemote

  implicit def fs(implicit configuration: ScoobiConfiguration) = FileSystem.get(configuration)

  def moveToRemote(file: File, keep: Boolean = false)(implicit configuration: ScoobiConfiguration) = {
    if (isRemote) {
      FileSystems.fileSystem.copyFromLocalFile(!keep, new Path(file.getPath), new Path(configuration.workingDirectory+file.getName))
      new File(configuration.workingDirectory+file.getName)
    } else file
  }
  def registerFile(file: File, keep: Boolean = false)(implicit configuration: ScoobiConfiguration) = {
    if (!keep) configuration.addValues("scoobi.test.files", file.getPath)
    file
  }
  private def deleteFiles(files: Seq[File])(implicit configuration: ScoobiConfiguration) {
    if (isRemote)
      files.foreach(f => TempFiles.deleteFile(f, isRemote))
    files.foreach(f => TempFiles.deleteFile(f))
  }
}

object TestFiles extends TestFiles

import TestFiles._

class InputTestFile[S](ls: Seq[String], mapping: String => S)
                      (implicit configuration: ScoobiConfiguration, m: Manifest[S], w: WireFormat[S]) {

  lazy val file = createTempFile("test.input")

  def inputLines = fromTextFile(TempFiles.writeLines(file, ls, isRemote))
  def map[T : Manifest : WireFormat](f: S => T) = new InputTestFile(ls, f compose mapping)
  def collect[T : Manifest : WireFormat](f: PartialFunction[S, T]) = new InputTestFile(ls, f compose mapping)
  def lines: DList[S] = inputLines.map(mapping)
}

case class InputStringTestFile(ls: Seq[String])
                              (implicit configuration: ScoobiConfiguration) extends InputTestFile[String](ls, identity) {
  /** Optimisation: in this case no mapping is necessary (see issue 25)*/
  override def lines: DList[String] = inputLines
}

case class OutputTestFile[T](list: DList[T])
                            (implicit configuration: ScoobiConfiguration, m: Manifest[T], w: WireFormat[T]) {

  lazy val outputDir  = TestFiles.createTempDir("test.output")
  lazy val outputPath = TempFiles.path(outputDir, isRemote)
  def outputFiles     = getFiles(outputDir)

  lazy val lines: Either[String, Seq[String]] = {
    persist(configuration)(toTextFile(list, outputPath, overwrite = true))
    if (outputFiles.isEmpty) Left("There are no output files in "+ outputDir.getName)
    else                     Right(Source.fromFile(outputFiles.head).getLines.toSeq)
  }
}

