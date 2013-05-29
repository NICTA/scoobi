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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, Job}

import org.apache.avro.{Schema, AvroTypeException}
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.mapred.{AvroKey, FsInput}
import org.apache.avro.mapreduce.{AvroKeyRecordReader, AvroKeyInputFormat}
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.file.DataFileReader

import core._
import impl.plan.DListImpl
import impl.ScoobiConfiguration._
import impl.io.Helper
import AvroParsingImplicits._
/**
 * Functions for materialising distributed lists by loading Avro files
 */
trait AvroInput {

  /** Create a new DList from the contents of one or more Avro files. The type of the DList must conform to
    * the schema types allowed by Avro, as constrained by the 'AvroSchema' type class. In the case of a directory
    * being specified, the input forms all the files in that directory. */
  def fromAvroFile[A : WireFormat : AvroSchema](paths: String*): DList[A] = fromAvroFile(paths)


  /** Create a new DList from the contents of a list of one or more Avro files. The type of the
    * DList must conform to the schema types allowed by Avro, as constrained by the 'AvroSchema' type
    * class. In the case of a directory being specified, the input forms all the files in
    * that directory. */
  def fromAvroFile[A : WireFormat : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck): DList[A] =
    DListImpl(source(paths, checkSchemas, check)(implicitly[AvroSchema[A]]))

  def source[A : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck) = {
    val schema = implicitly[AvroSchema[A]]
    val converter = new InputConverter[AvroKey[schema.AvroType], NullWritable, A] {
      def fromKeyValue(context: InputContext, k: AvroKey[schema.AvroType], v: NullWritable) = schema.fromAvro(k.datum)
    }
    new AvroDataSource[schema.AvroType, A](paths.map(p => new Path(p)), converter, schema, checkSchemas, check)
  }
}
object AvroInput extends AvroInput

case class AvroDataSource[K, A](paths: Seq[Path],
                                inputConverter: InputConverter[AvroKey[K], NullWritable, A],
                                schema: AvroSchema[A],
                                checkSchemas: Boolean = true,
                                check: Source.InputCheck = Source.defaultInputCheck) extends DataSource[AvroKey[K], NullWritable, A] {

  private implicit lazy val logger = LogFactory.getLog("scoobi.AvroInput")

  override def toString = "Avro("+id+")"+paths.mkString("\n", "\n", "\n")

  val inputFormat =
    if (schema.getType == Schema.Type.NULL) classOf[GenericAvroKeyInputFormat[K]]
    else                                    classOf[AvroKeyInputFormat[K]]

  /**
   * Check if the input paths exist and optionally that the reader schema is compatible with the written schema.
   * For efficiency, the schema checking will only check one file per dir
   */
  def inputCheck(implicit sc: ScoobiConfiguration) {
    check(paths, sc)
    paths.foreach(checkInputSchema)
  }

  protected def checkInputSchema(p: Path)(implicit sc: ScoobiConfiguration) {
    if (checkSchemas && schema.getType != Schema.Type.NULL) {
      val fileStats = Helper.getFileStatus(p)(sc)
      Helper.getSingleFilePerDir(fileStats)(sc) foreach { filePath =>
        val avroFile = new FsInput(filePath, sc)
        try {
          val writerSchema = DataFileReader.openReader(avroFile, new GenericDatumReader[K]()).getSchema

          // resolve the two schemas and get errors if any
          ResolvingDecoder.resolve(writerSchema, schema.schema) match {
            case sym: Symbol if sym.getErrors.nonEmpty => {
              val errors = sym.getErrors.map(_.msg).mkString("\n")
              throw new AvroTypeException(s"Incompatible reader and writer schemas. Reader schema '$schema'. Writer schema '$writerSchema'. Errors:\n$errors")
            }
            case sym: Symbol => ()
            case _           => throw new ClassCastException("Avro ResolvingDecoder api has changed. Expecting a Symbol " +
              "class to be returned from ResolvingDecoder.resolve(writerSchema, readerSchema)")
          }

        } finally avroFile.close()
      }
    }
  }

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    paths.foreach(p => FileInputFormat.addInputPath(job, p))
    job.getConfiguration.set("avro.schema.input.key", schema.toString)
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = paths.map(p => Helper.pathSize(p)(sc)).sum
}

class GenericAvroKeyInputFormat[T] extends AvroKeyInputFormat[T] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    new RecordReader[AvroKey[T], NullWritable] {
      private var delegate: AvroKeyRecordReader[T] = _

      def initialize(split: InputSplit, context: TaskAttemptContext) {
        val schema = DataFileReader.openReader(new FsInput(split.asInstanceOf[FileSplit].getPath, context.getConfiguration), new GenericDatumReader[T]()).getSchema
        delegate = new AvroKeyRecordReader[T](schema)
        delegate.initialize(split, context)
      }

      def nextKeyValue = delegate.nextKeyValue
      def getCurrentKey = delegate.getCurrentKey
      def getCurrentValue = delegate.getCurrentValue
      def getProgress = delegate.getProgress
      def close = delegate.close

    }
  }

}
