package com.nicta.scoobi.testing

import java.io.File
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.FileSystem
import com.nicta.scoobi.{Scoobi, ScoobiConfiguration}
import Scoobi._
import io.Source

/**
 * This trait provides a test input file, created as a temporary file on the local file system and copied to the cluster
 * if the configuration is a remote one.
 *
 * It also creates an output directory and
 */
trait TestFiles {

  def configuration: ScoobiConfiguration

  implicit lazy val fs = FileSystem.get(configuration)
  lazy val input = {
    // before the creation of the input we set the mapred local dir.
    // this setting is necessary to avoid xml parsing when several jobs are executing concurrently and
    // trying to access the job.xml file
    configuration.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, localDir.getPath+"/")
    createTempFile("test.input")
  }
  lazy val localDir  = createTempDir("test.local")
  lazy val outputDir = createTempDir("test.output")

  lazy val outputPath = TempFiles.path(outputDir, isRemote)

  def isRemote = configuration.isRemote

  def deleteFiles {
    val files = Seq(input, localDir, outputDir)
    if (isRemote)
      files foreach TempFiles.deleteFile
    files foreach TempFiles.deleteFile
  }

  def createTempFile(prefix: String) = TempFiles.createTempFile(prefix+configuration.jobId)
  def createTempDir(prefix: String)  = TempFiles.createTempDir(prefix+configuration.jobId)
  def path(file: File)               = TempFiles.path(file, isRemote)
  def inputLines(lines: Seq[String]) = fromTextFile(TempFiles.writeLines(input, lines, isRemote))
  def outputFiles                    = TempFiles.getFiles(outputDir, isRemote)
  def outputLines                    = Source.fromFile(outputFiles.head).getLines.toSeq
}

