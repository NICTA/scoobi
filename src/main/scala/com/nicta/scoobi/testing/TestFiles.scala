package com.nicta.scoobi.testing

import java.io.File
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.FileSystem
import com.nicta.scoobi.{Scoobi, ScoobiConfiguration}
import Scoobi._
import io.Source

/**
 * This trait creates input and output files which are temporary
 * The files paths are registered in the configuration so that they can be deleted when the job has been executed
 */
trait TestFiles {

  def prepare(implicit configuration: ScoobiConfiguration) {
    // before the creation of the input we set the mapred local dir.
    // this setting is necessary to avoid xml parsing when several jobs are executing concurrently and
    // trying to access the job.xml file
    configuration.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, createTempDir("test.local").getPath+"/")
  }

  def createTempFile(prefix: String, keep: Boolean)(implicit configuration: ScoobiConfiguration) =
    registerFile(TempFiles.createTempFile(prefix+configuration.jobId), keep)

  def createTempFile(prefix: String)(implicit configuration: ScoobiConfiguration) =
    registerFile(TempFiles.createTempFile(prefix+configuration.jobId))

  def createTempDir(prefix: String, keep: Boolean)(implicit configuration: ScoobiConfiguration) =
    registerFile(TempFiles.createTempDir(prefix+configuration.jobId), keep)

  def createTempDir(prefix: String)(implicit configuration: ScoobiConfiguration) =
    registerFile(TempFiles.createTempDir(prefix+configuration.jobId))

  def getFiles(dir: File)(implicit configuration: ScoobiConfiguration): Seq[File] =
    TempFiles.getFiles(dir, isRemote)(fs)

  def getFiles(dir: String)(implicit configuration: ScoobiConfiguration): Seq[File] =
    getFiles(new File(dir))

  def deleteFiles(implicit configuration: ScoobiConfiguration) {
    deleteFiles(configuration.get("scoobi.test.files").split(",").toSeq.map(new File(_)))
  }

  def path(path: String)(implicit configuration: ScoobiConfiguration): String = TempFiles.path(new File(path), isRemote)
  def path(file: File)(implicit configuration: ScoobiConfiguration): String = TempFiles.path(file, isRemote)

  def isRemote(implicit configuration: ScoobiConfiguration) = configuration.isRemote

  implicit def fs(implicit configuration: ScoobiConfiguration) = FileSystem.get(configuration)

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

case class InputTestFile[S](ls: Seq[String], mapping: String => S, keepFile: Boolean = false)
                           (implicit configuration: ScoobiConfiguration, m: Manifest[S], w: WireFormat[S]) {

  lazy val file = createTempFile("test.input", keep = keepFile)

  def keep = copy(keepFile = true)
  def inputLines = fromTextFile(TempFiles.writeLines(file, ls, isRemote))
  def map[T : Manifest : WireFormat](f: S => T) = InputTestFile(ls, f compose mapping)
  def collect[T : Manifest : WireFormat](f: PartialFunction[S, T]) = InputTestFile(ls, f compose mapping)
  def lines: DList[S] = inputLines.map(mapping)
}

case class OutputTestFile[T](list: DList[T], keepDir: Boolean = false)
                            (implicit configuration: ScoobiConfiguration, m: Manifest[T], w: WireFormat[T]) {

  lazy val outputDir  = TestFiles.createTempDir("test.output", keep = keepDir)
  lazy val outputPath = TempFiles.path(outputDir, isRemote)
  def outputFiles     = getFiles(outputDir)

  def keep = copy(keepDir = true)

  lazy val lines: Either[String, Seq[String]] = {
    persist(configuration)(toTextFile(list, outputPath, overwrite = true))
    if (outputFiles.isEmpty) Left("There are no output files in "+ outputDir.getName)
    else                     Right(Source.fromFile(outputFiles.head).getLines.toSeq)
  }
}

case class OutputTestFiles[T1, T2](list1: DList[T1],
                                   list2: DList[T2], keepDir: Boolean = false)
                                  (implicit configuration: ScoobiConfiguration,
                                   m1: Manifest[T1], w1: WireFormat[T1],
                                   m2: Manifest[T2], w2: WireFormat[T2]) {

  lazy val outputDir  = TestFiles.createTempDir("test.output", keep = keepDir)
  lazy val outputPath = TempFiles.path(outputDir, isRemote)
  def outputFiles     = getFiles(outputDir)

  def keep = copy(keepDir = true)

  lazy val lines: Either[String, (Seq[String], Seq[String])] = {

    persist(configuration)(toTextFile(list1, outputPath+"/1", overwrite = true),
                           toTextFile(list2, outputPath+"/2", overwrite = true))

    val files = outputFiles
    if (files.isEmpty)        Left("There are no output files in "+outputDir.getName)
    else if (files.size == 1) Left("There is only one output file in "+outputDir.getName+" instead of 2")
    else                            {
      val (f1, f2) = (getFiles(outputDir+"/1").head, getFiles(outputDir+"/2").head)
      Right((getLines(f1), getLines(f2)))
    }
  }

  private def getFile(i: Int) = getLines(outputFiles(i))
  private def getLines(file: File) = Source.fromFile(file).getLines.toSeq
}
