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

/** Smart functions for persisting distributed lists by storing them as Avro files. */
object AvroOutput {
  lazy val logger = LogFactory.getLog("scoobi.AvroOutput")


  /** Specify a distributed list to be persistent by storing it to disk as an Avro File. */
  def toAvroFile[B : AvroSchema](dl: DList[B], path: String, overwrite: Boolean = false) =
    dl.addSink(avroSink(path, overwrite))

  def avroSink[B : AvroSchema](path: String, overwrite: Boolean = false) = {
    val sch = implicitly[AvroSchema[B]]
    val converter = new OutputConverter[AvroKey[sch.AvroType], NullWritable, B] {
      def toKeyValue(x: B)(implicit configuration: Configuration) = (new AvroKey(sch.toAvro(x)), NullWritable.get)
    }

    new DataSink[AvroKey[sch.AvroType], NullWritable, B] {

      protected val output = new Path(path)

      def outputFormat(implicit sc: ScoobiConfiguration)     = classOf[AvroKeyOutputFormat[sch.AvroType]]
      def outputKeyClass(implicit sc: ScoobiConfiguration)   = classOf[AvroKey[sch.AvroType]]
      def outputValueClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]

      def outputCheck(implicit sc: ScoobiConfiguration) {
        if (Helper.pathExists(output)(sc.configuration) && !overwrite) {
          throw new FileAlreadyExistsException("Output path already exists: " + output)
        } else {
          logger.info("Output path: " + output.toUri.toASCIIString)
          logger.debug("Output Schema: " + sch.schema)
        }
      }
      def outputPath(implicit sc: ScoobiConfiguration) = Some(output)

      def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
        job.getConfiguration.set("avro.schema.output.key", sch.schema.toString)
      }

      override def outputSetup(implicit configuration: Configuration) {
        if (Helper.pathExists(output)(configuration) && overwrite) {
          logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
          Helper.deletePath(output)(configuration)
        }
      }

      lazy val outputConverter = converter

      override def toSource: Option[Source] = Some(AvroInput.source(Seq(path), checkSchemas = false)(implicitly[AvroSchema[B]]))
    }

  }
}
