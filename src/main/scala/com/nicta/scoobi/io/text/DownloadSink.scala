package com.nicta.scoobi.io.text

import com.nicta.scoobi.core.{ScoobiConfiguration, Sink}
import com.nicta.scoobi.core.Data.ids
import org.apache.hadoop.mapreduce.{RecordWriter, Job}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.SequenceFile.CompressionType

/**
 * This is a dummy sink just used to collect files downloaded in map tasks
 *
 * The map task must be a parallelDo like this:
 *
 * def download = (path: String, InputOutputContext) => {
 *   // get the output directory for the current map task
 *   val outputDir = FileOutputFormat.getWorkOutputPath(context.context)
 *   val outDir = outputDir.toString.replace("file:", "")
 *   logger.debug("output dir is "+outDir)
 *
 *   // download the file
 *   // ...
 * }
 *
 * val sink = new DownloadSink("target/test", (_:String).startsWith("source"))
 *
 * val fileNames: DList[String] = ???
 * fileNames.parallelDo(download).addSink(sink).persist
 *
 * The downloaded files will be collected from the working directory of the map task and go to "target/test" based on their path
 */

class DownloadSink(target: String, isDownloadedFile: Path => Boolean, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) extends Sink {
  val id = ids.get
  lazy val stringId = id.toString
  private val delegate = new TextFileSink[Unit](target, overwrite, check)

  def outputFormat(implicit sc: ScoobiConfiguration)              = delegate.outputFormat
  def outputKeyClass(implicit sc: ScoobiConfiguration)            = delegate.outputKeyClass
  def outputValueClass(implicit sc: ScoobiConfiguration)          = delegate.outputValueClass
  def outputConverter                                             = delegate.outputConverter
  def outputCheck(implicit sc: ScoobiConfiguration)               = delegate.outputCheck
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) = delegate.outputConfigure(job)
  def outputPath(implicit sc: ScoobiConfiguration): Option[Path]  = delegate.outputPath
  def outputSetup(implicit sc: ScoobiConfiguration)               = delegate.outputSetup
  def outputTeardown(implicit sc: ScoobiConfiguration)            = delegate.outputTeardown
  def configureCompression(configuration: Configuration): Sink    = this
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK): Sink = this

  private[scoobi] def isCompressed: Boolean = false

  private [scoobi] def write(values: Traversable[_], recordWriter: RecordWriter[_,_])(implicit configuration: Configuration) {}

  override def isSinkResult(tag: Int) = isDownloadedFile

}
