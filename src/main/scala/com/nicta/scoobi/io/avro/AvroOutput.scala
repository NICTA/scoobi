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
package io
package avro

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat

import core._
import impl.io.Helper
import org.apache.hadoop.conf.Configuration
import impl.ScoobiConfigurationImpl
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType

/** Smart functions for persisting distributed lists by storing them as Avro files. */
object AvroOutput {

  /** Specify a distributed list to be persistent by storing it to disk as an Avro File. */
  def toAvroFile[B](dl: DList[B], path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit schema: AvroSchema[B], sc: ScoobiConfiguration) =
    dl.addSink(avroSink(path, overwrite, checkpoint))

  def avroSink[B](path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit sc: ScoobiConfiguration, schema: AvroSchema[B]) = {
    val converter = new OutputConverter[AvroKey[schema.AvroType], NullWritable, B] {
      def toKeyValue(x: B)(implicit configuration: Configuration) = (new AvroKey(schema.toAvro(x)), NullWritable.get)
    }
    AvroSink[schema.AvroType, B](schema, path, converter, overwrite, Checkpoint.create(Some(path), checkpoint))
  }

}

case class AvroSink[K, B](schema: AvroSchema[B],
                          path: String,
                          outputConverter: OutputConverter[AvroKey[K], NullWritable, B],
                          overwrite: Boolean = false,
                          checkpoint: Option[Checkpoint] = None,
                          compression: Option[Compression] = None) extends DataSink[AvroKey[K], NullWritable, B] with SinkSource {

  private lazy val logger = LogFactory.getLog("scoobi.AvroOutput")

  lazy val output = new Path(path)

  def outputFormat(implicit sc: ScoobiConfiguration)     = classOf[AvroKeyOutputFormat[K]]
  def outputKeyClass(implicit sc: ScoobiConfiguration)   = classOf[AvroKey[K]]
  def outputValueClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]

  def outputCheck(implicit sc: ScoobiConfiguration) {
    if (Helper.pathExists(output)(sc.configuration) && !overwrite) {
      throw new FileAlreadyExistsException("Output path already exists: " + output)
    } else {
      logger.info("Output path: " + output.toUri.toASCIIString)
      logger.debug("Output Schema: " + schema.schema)
    }
  }
  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)

  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    job.getConfiguration.set("avro.schema.output.key", schema.schema.toString)
  }

  override def outputSetup(implicit configuration: Configuration) {
    if (Helper.pathExists(output)(configuration) && overwrite) {
      logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
      Helper.deletePath(output)(configuration)
    }
  }

  def toSource: Source = AvroInput.source(Seq(path), checkSchemas = false)(schema)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}
