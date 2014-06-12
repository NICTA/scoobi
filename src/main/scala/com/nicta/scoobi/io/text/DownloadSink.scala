package com.nicta.scoobi.io.text

import com.nicta.scoobi.core._
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{JobContext, Job}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.NullWritable
import com.nicta.scoobi.core
import com.nicta.scoobi.core.Compression

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
class DownloadSink(target: String, isDownloadedFile: Path => Boolean, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) extends DataSink[NullWritable, NullWritable, NullWritable] {
  private val delegate =
    if (overwrite) new OverwritableTextSink[NullWritable](target, overwrite, check)
    else           new TextFileSink[NullWritable](target, overwrite, check)

  override def isSinkResult(tag: Int) = isDownloadedFile

  override def compressWith(codec: CompressionCodec, compressionType: CompressionType) = delegate.compressWith(codec, compressionType)
  override def outputPath(implicit sc: core.ScoobiConfiguration): Option[Path] = delegate.outputPath
  override def compression: Option[Compression] = None
  override def outputConfigure(job: Job)(implicit sc: core.ScoobiConfiguration) = delegate.outputConfigure(job)
  override def outputCheck(implicit sc: core.ScoobiConfiguration) = delegate.outputCheck
  override def outputConverter: OutputConverter[NullWritable, NullWritable, NullWritable] = delegate.outputConverter
  override def outputValueClass(implicit sc: core.ScoobiConfiguration) = delegate.outputValueClass
  override def outputKeyClass(implicit sc: core.ScoobiConfiguration) = delegate.outputKeyClass
  override def outputFormat(implicit sc: core.ScoobiConfiguration) = delegate.outputFormat
}

class OverwritableTextSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, compression: Option[Compression] = None) extends
  TextFileSink[A](path, overwrite, check, compression)  {
  override def outputFormat(implicit sc: ScoobiConfiguration) =
    classOf[OverwritableTextOutputFormat[NullWritable, A]]
}

class OverwritableTextOutputFormat[K, V] extends TextOutputFormat[K, V] {
  override def checkOutputSpecs(job: JobContext) {
    val outDir: Path = FileOutputFormat.getOutputPath(job)
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.")
    }
  }
}
