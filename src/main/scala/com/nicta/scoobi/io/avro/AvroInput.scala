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

import java.io.IOException

import org.apache.commons.logging.LogFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job

import org.apache.avro.AvroTypeException
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.mapred.{AvroKey, FsInput}
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.file.DataFileReader

import core._
import application.ScoobiConfiguration

/** Smart functions for materializing distributed lists by loading Avro files. */
object AvroInput extends AvroParsingImplicits {
  lazy val logger = LogFactory.getLog("scoobi.AvroInput")


  /** Create a new DList from the contents of one or more Avro files. The type of the DList must conform to
    * the schema types allowed by Avro, as constrained by the 'AvroSchema' type class. In the case of a directory
    * being specified, the input forms all the files in that directory. */
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: String*): DList[A] = fromAvroFile(List(paths: _*))


  /** Create a new DList from the contents of a list of one or more Avro files. The type of the
    * DList must conform to the schema types allowed by Avro, as constrained by the 'AvroSchema' type
    * class. In the case of a directory being specified, the input forms all the files in
    * that directory. */
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: List[String], checkSchemas: Boolean = true): DList[A] = {
    val sch = implicitly[AvroSchema[A]]
    val converter = new InputConverter[AvroKey[sch.AvroType], NullWritable, A] {
      def fromKeyValue(context: InputContext, k: AvroKey[sch.AvroType], v: NullWritable) = sch.fromAvro(k.datum)
    }
    val source = new DataSource[AvroKey[sch.AvroType], NullWritable, A] {

      // default config until its set through inputConfigure or outputConfigure
      private var config: Configuration = new Configuration

      private val inputPaths = paths.map(p => new Path(p))

      val inputFormat = classOf[AvroKeyInputFormat[sch.AvroType]]

      /** Check if the input paths exist and optionally that the reader schema is compatible with the written schema.
        * For efficiency, the schema checking will only check one file per dir. */
      def inputCheck(sc: ScoobiConfiguration) {
        inputPaths foreach { p =>
          val fileStats = Helper.getFileStatus(p)(sc)

          if (fileStats.size > 0) {
            logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)(sc)) + ")")
            logger.debug("Input schema: " + sch.schema)
          } else {
            throw new IOException("Input path " + p + " does not exist.")
          }

          if (checkSchemas) {
            // for efficiency, only check one file per dir
            Helper.getSingleFilePerDir(fileStats)(sc) foreach { filePath =>
              val avroFile = new FsInput(filePath, sc);
              try {
                val writerSchema = DataFileReader.openReader(avroFile, new GenericDatumReader[AnyRef]()).getSchema
                val readerSchema = sch.schema

                // resolve the two schemas.
                val symbol = ResolvingDecoder.resolve(writerSchema, readerSchema) match {
                  case sym: Symbol => sym
                  case _ => throw new ClassCastException("Avro ResolvingDecoder api has changed. Expecting a Symbol " +
                    "class to be returned from ResolvingDecoder.resolve(writerSchema, readerSchema)")
                }

                // create an error string for any schema resolution errors
                val errors = symbol.getErrors.map(_.msg).mkString("", "\n", "")
                if (errors.length > 0)
                  throw new AvroTypeException("Incompatible reader and writer schemas. Reader schema '" +
                    readerSchema + "'. Writer schema '" + writerSchema + "'. Errors:\n" + errors)
              } finally {
                avroFile.close();
              }
            }
          }
        }
      }

      def inputConfigure(job: Job) = {
        config = job.getConfiguration
        inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
        job.getConfiguration.set("avro.schema.input.key", sch.schema.toString)
      }

      def inputSize: Long = inputPaths.map(p => Helper.pathSize(p)(config)).sum

      lazy val inputConverter = converter
    }

    DList.fromSource(source)
  }
}
