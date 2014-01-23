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
package sequence

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job

import core._
import impl.ScoobiConfiguration._
import impl.io.Files
import org.apache.hadoop.conf.Configuration
import impl.ScoobiConfigurationImpl
import core.ScoobiConfiguration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType

/** Smart functions for persisting distributed lists by storing them as Sequence files. */
trait SequenceOutput {

  /**
   * Specify a distributed list to be persistent by converting its elements to Writables and storing it
   * to disk as the "key" component in a Sequence File.
   *  @deprecated(message="use list.keyToSequenceFile(...) instead", since="0.7.0")
   */
  def keyToSequenceFile[K](dl: DList[K], path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convK: SeqSchema[K]) =
    dl.addSink(keySchemaSequenceFile(path, overwrite, check))

  def keySchemaSequenceFile[K](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convK: SeqSchema[K]) = {

    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = classOf[NullWritable]

    val converter = new InputOutputConverter[convK.SeqType, NullWritable, K] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: NullWritable) = convK.fromWritable(k)
      def toKeyValue(kv: K)(implicit configuration: Configuration) = kv match {
        case (k1, _) => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
        case k1      => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
      }
    }
    new SeqSink[convK.SeqType, NullWritable, K](path, keyClass, valueClass, converter, overwrite, check)
  }

  /**
   * Specify a distributed list to be persistent by converting its elements to Writables and storing it
   * to disk as the "value" component in a Sequence File.
   *  @deprecated(message="use list.valueToSequenceFile(...) instead", since="0.7.0")
   */
  def valueToSequenceFile[V](dl: DList[V], path: String, overwrite: Boolean = false,
                             check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false)(implicit convV: SeqSchema[V], sc: ScoobiConfiguration) =
    dl.addSink(valueSchemaSequenceFile(path, overwrite, check, checkpoint))

  def valueSchemaSequenceFile[V](path: String, overwrite: Boolean = false,
                                 check: Sink.OutputCheck = Sink.defaultOutputCheck,
                                 checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit convV: SeqSchema[V], sc: ScoobiConfiguration) = {
    val keyClass = classOf[NullWritable]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[NullWritable, convV.SeqType, V] {
      def fromKeyValue(context: InputContext, k: NullWritable, v: convV.SeqType) = convV.fromWritable(v)
      def toKeyValue(kv: V)(implicit configuration: Configuration) = kv match {
        case (_,v) => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
        case v     => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
      }
    }
    new SeqSink[NullWritable, convV.SeqType, V](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

  /**
   * Specify a distributed list to be persistent by converting its elements to Writables and storing it
   * to disk as "key-values" in a Sequence File
   * @deprecated(message="use list.toSequenceFile(...) instead", since="0.7.0")
   */
  def toSequenceFile[K, V](dl: DList[(K, V)], path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false)(implicit convK: SeqSchema[K], convV: SeqSchema[V], sc: ScoobiConfiguration) =
    dl.addSink(schemaSequenceSink(path, overwrite, check, checkpoint)(convK, convV, sc))

  def schemaSequenceSink[K, V](path: String, overwrite: Boolean = false,
                               check: Sink.OutputCheck = Sink.defaultOutputCheck,
                               checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit convK: SeqSchema[K], convV: SeqSchema[V], sc: ScoobiConfiguration)= {

    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: convV.SeqType) = (convK.fromWritable(k), convV.fromWritable(v))
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }
    new SeqSink[convK.SeqType, convV.SeqType, (K, V)](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

  def sequenceSink[K <: Writable, V <: Writable](path: String, overwrite: Boolean = false,
                                                 check: Sink.OutputCheck = Sink.defaultOutputCheck,
                                                 checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(
      implicit mk: Manifest[K], mv: Manifest[V], sc: ScoobiConfiguration) = {
    val keyClass = mk.runtimeClass.asInstanceOf[Class[K]]
    val valueClass = mv.runtimeClass.asInstanceOf[Class[V]]

    val converter = new InputOutputConverter[K, V, (K, V)] {
      def fromKeyValue(context: InputContext, k: K, v: V) = (k, v)
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (kv._1, kv._2)
    }
    new SeqSink[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

}

object SequenceOutput extends SequenceOutput

/** class that abstracts all the common functionality of persisting to sequence files. */
case class SeqSink[K, V, B](path: String,
                            keyClass: Class[K],
                            valueClass: Class[V],
                            outputConverter: InputOutputConverter[K, V, B],
                            overwrite: Boolean,
                            check: Sink.OutputCheck = Sink.defaultOutputCheck,
                            checkpoint: Option[Checkpoint] = None,
                            compression: Option[Compression] = None) extends DataSink[K, V, B] with SinkSource {

  private lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  protected val output = new Path(path)

  def toSource = new SeqSource(Seq(path), SequenceInput.defaultSequenceInputFormat, outputConverter)

  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[SequenceFileOutputFormat[K, V]]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = keyClass
  def outputValueClass(implicit sc: ScoobiConfiguration) = valueClass

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

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}
