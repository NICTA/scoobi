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
package com.nicta
package scoobi
package io
package text

import com.nicta.scoobi.core._
import org.apache.hadoop.io.NullWritable
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce._
import com.nicta.scoobi.impl.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import com.nicta.scoobi.impl.ScoobiConfigurationImpl
import com.nicta.scoobi.core.Compression
import com.nicta.scoobi.impl.util.DistCache

case class TextFilePartitionedSink[K : Manifest, V : Manifest](
  path: String,
  partition: K => String,
  overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, compression: Option[Compression] = None) extends DataSink[K, V, (K, V)] {

  private lazy val logger = LogFactory.getLog("scoobi.TextFilePartitionedSink")

  private val output = new Path(path)

  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[PartitionedTextOutputFormat[K, V]]
  def outputKeyClass(implicit sc: ScoobiConfiguration)   = TextFileSink.runtimeClass[K]
  def outputValueClass(implicit sc: ScoobiConfiguration) = TextFileSink.runtimeClass[V]

  def outputCheck(implicit sc: ScoobiConfiguration) {
    check(output, overwrite, sc)
  }

  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}

  override def outputSetup(implicit sc: ScoobiConfiguration) {
    super.outputSetup(sc)

    if (Files.pathExists(output)(sc.configuration) && overwrite) {
      logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
      Files.deletePath(output)(sc.configuration)
    }
    DistCache.pushObject[K => String](sc.configuration, partition, TextFilePartitionedSink.functionTag(sc.configuration, outputPath.getOrElse(id).toString))
  }

  lazy val outputConverter = new OutputConverter[K, V, (K, V)] {
    def toKeyValue(x: (K, V))(implicit configuration: Configuration) = x
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}

object TextFilePartitionedSink {

  /**
   * we need a specific tag to save the partition function in the distributed cache
   * we use the mapred.work.output.dir property to make sure that each function will be associated
   * to the correct PartitionedTextOutputFormat
   */
  def functionTag(configuration: Configuration, defaultWorkDir: String = "-") = {
    val outputDir = configuration.get("mapred.work.output.dir", defaultWorkDir)
    val withoutProtocol = new Path(outputDir).toString.split("/").drop(2).mkString("/")
    "pathPartitionFunction_"+withoutProtocol
  }

}

class PartitionedTextOutputFormat[K, V] extends FileOutputFormat[K, V] {

  def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    lazy val partitionFunctionTag = TextFilePartitionedSink.functionTag(context.getConfiguration)
    lazy val partitionFunction = DistCache.pullObject[K => String](context.getConfiguration, partitionFunctionTag)
    lazy val outputDir = FileOutputFormat.getOutputPath(context)

    new RecordWriter[K, V] {
      private val recordWriters =
        new collection.mutable.HashMap[Path, RecordWriter[NullWritable, V]]

      def write(key: K, value: V) {
        val finalPath = generatePathForKeyValue(key, value, outputDir, partitionFunction)(context.getConfiguration)
        val rw = recordWriters.get(finalPath) match {
          case None    => val newWriter = getBaseRecordWriter(context, finalPath); recordWriters.put(finalPath, newWriter); newWriter
          case Some(x) => x
        }
        rw.write(NullWritable.get, value)
      }

      def close(context: TaskAttemptContext) {
        recordWriters.values.foreach(_.close(context))
        recordWriters.clear
      }
    }
  }

  protected def generatePathForKeyValue(key: K, value: V, outputDir: Path, partitionFunction: Option[K => String])(configuration: Configuration): Path = {
    new Path(outputDir, partitionFunction.map(_(key)).getOrElse(key.toString))
  }

  protected def getBaseRecordWriter(context: TaskAttemptContext, path: Path): RecordWriter[NullWritable, V] =
    new TextOutputFormat[NullWritable, V] {
      override def getOutputCommitter(context: TaskAttemptContext) = new FileOutputCommitter(path, context)
    }.getRecordWriter(context)
}