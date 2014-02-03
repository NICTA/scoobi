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

import core._
import org.apache.hadoop.io.NullWritable
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Job
import impl.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import impl.ScoobiConfigurationImpl


case class TextFileSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, compression: Option[Compression] = None) extends DataSink[NullWritable, A, A] {
  private lazy val logger = LogFactory.getLog("scoobi.TextOutput")

  private val output = new Path(path)

  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[TextOutputFormat[NullWritable, A]]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]
  def outputValueClass(implicit sc: ScoobiConfiguration) = TextFileSink.runtimeClass[A]
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
  }

  lazy val outputConverter = new OutputConverter[NullWritable, A, A] {
    def toKeyValue(x: A)(implicit configuration: Configuration) = (NullWritable.get, x)
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}

object TextFileSink {
  def runtimeClass[A : Manifest] = implicitly[Manifest[A]] match {
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
}

