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

import application.DListPersister
import core._
import application.ScoobiConfiguration

/** Smart functions for persisting distributed lists by storing them as Avro files. */
object AvroOutput {
  lazy val logger = LogFactory.getLog("scoobi.AvroOutput")


  /** Specify a distributed list to be persistent by storing it to disk as an Avro File. */
  def toAvroFile[B : AvroSchema](dl: DList[B], path: String, overwrite: Boolean = false): DListPersister[B] = {
    val sch = implicitly[AvroSchema[B]]

    val converter = new OutputConverter[AvroKey[sch.AvroType], NullWritable, B] {
      def toKeyValue(x: B) = (new AvroKey(sch.toAvro(x)), NullWritable.get)
    }

    val sink = new DataSink[AvroKey[sch.AvroType], NullWritable, B] {
      protected val outputPath = new Path(path)

      val outputFormat = classOf[AvroKeyOutputFormat[sch.AvroType]]
      val outputKeyClass = classOf[AvroKey[sch.AvroType]]
      val outputValueClass = classOf[NullWritable]

      def outputCheck(sc: ScoobiConfiguration) {
        if (Helper.pathExists(outputPath)(sc)) {
          if (overwrite) {
            logger.info("Deleting the pre-existing output path: " + outputPath.toUri.toASCIIString)
            Helper.deletePath(outputPath)(sc)
          } else {
            throw new FileAlreadyExistsException("Output path already exists: " + outputPath)
          }
        } else {
          logger.info("Output path: " + outputPath.toUri.toASCIIString)
          logger.debug("Output Schema: " + sch.schema)
        }
      }

      def outputConfigure(job: Job) {
        FileOutputFormat.setOutputPath(job, outputPath)
        job.getConfiguration.set("avro.schema.output.key", sch.schema.toString)
      }

      lazy val outputConverter = converter
    }

    new DListPersister(dl, sink)
  }
}
