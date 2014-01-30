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
package text

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

import core._
import impl.io.Files
import avro.AvroInput
import org.apache.hadoop.conf.Configuration
import impl.ScoobiConfigurationImpl
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType

/** Smart functions for persisting distributed lists by storing them as text files. */
trait TextOutput {

  /**
   * Persist a distributed list as a text file.
   * @deprecated(message="use list.toTextFile(...) instead", since="0.7.0")
   */
  def toTextFile[A : Manifest](dl: DList[A], path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    dl.addSink(textFileSink(path, overwrite, check))

  /** Persist a distributed lists of 'Products' (e.g. Tuples) as a delimited text file. */
  def listToDelimitedTextFile[A <: Product : Manifest](dl: DList[A], path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) = {
    def anyToString(any: Any, sep: String): String = any match {
      case prod: Product => prod.productIterator.map(anyToString(_, sep)).mkString(sep)
      case _             => any.toString
    }
    (dl map { anyToString(_, sep) }).addSink(textFileSink[A](path, overwrite, check))
  }

  /** Persist a distributed object of 'Products' (e.g. Tuples) as a delimited text file. */
  def objectToDelimitedTextFile[A <: Product : Manifest](o: DObject[A], path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) = {
    def anyToString(any: Any, sep: String): String = any match {
      case prod: Product => prod.productIterator.map(anyToString(_, sep)).mkString(sep)
      case _             => any.toString
    }
    (o map { anyToString(_, sep) }).addSink(textFileSink[A](path, overwrite, check))
  }

  /**
   * SINKS
   */
  def textFileSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    new TextFileSink(path, overwrite, check)

  def textFilePartitionedSink[K : Manifest, V : Manifest](path: String,
                                                          partition: K => String,
                                                          overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    new TextFilePartitionedSink(path, partition, overwrite, check)(implicitly[Manifest[K]], implicitly[Manifest[V]])
}

object TextOutput extends TextOutput
