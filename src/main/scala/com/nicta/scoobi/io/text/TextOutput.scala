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
import impl.io.Helper
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

  def textFileSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    new TextFileSink(path, overwrite, check)

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
}
object TextOutput extends TextOutput

case class TextFileSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, compression: Option[Compression] = None) extends DataSink[NullWritable, A, A] {
  private lazy val logger = LogFactory.getLog("scoobi.TextOutput")

  private val output = new Path(path)

  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[TextOutputFormat[NullWritable, A]]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]
  def outputValueClass(implicit sc: ScoobiConfiguration) = implicitly[Manifest[A]] match {
    case Manifest.Boolean => classOf[java.lang.Boolean].asInstanceOf[Class[A]]
    case Manifest.Char    => classOf[java.lang.Character].asInstanceOf[Class[A]]
    case Manifest.Short   => classOf[java.lang.Short].asInstanceOf[Class[A]]
    case Manifest.Int     => classOf[java.lang.Integer].asInstanceOf[Class[A]]
    case Manifest.Long    => classOf[java.lang.Long].asInstanceOf[Class[A]]
    case Manifest.Float   => classOf[java.lang.Float].asInstanceOf[Class[A]]
    case Manifest.Double  => classOf[java.lang.Double].asInstanceOf[Class[A]]
    case Manifest.Byte    => classOf[java.lang.Byte].asInstanceOf[Class[A]]
    case mf               => mf.runtimeClass.asInstanceOf[Class[A]]
  }

  def outputCheck(implicit sc: ScoobiConfiguration) {
    check(output, overwrite, sc)
  }

  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}

  override def outputSetup(implicit configuration: Configuration) {
    if (Helper.pathExists(output)(configuration) && overwrite) {
      logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
      Helper.deletePath(output)(configuration)
    }
  }

  lazy val outputConverter = new OutputConverter[NullWritable, A, A] {
    def toKeyValue(x: A)(implicit configuration: Configuration) = (NullWritable.get, x)
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}
