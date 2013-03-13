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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

import core._
import impl.ScoobiConfiguration._
import impl.io.Helper
import avro.AvroInput
import sequence.SequenceInput.SeqSource
import org.apache.hadoop.conf.Configuration

/** Smart functions for persisting distributed lists by storing them as Sequence files. */
object SequenceOutput {
  lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "key" component in a Sequence File. */
  def convertKeyToSequenceFile[K](dl: DList[K], path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K]) =
    dl.addSink(keySchemaSequenceFile(path, overwrite))

  def keySchemaSequenceFile[K](path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K]) = {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.SeqType]]
    val valueClass = classOf[NullWritable]

    val converter = new OutputConverter[convK.SeqType, NullWritable, K] {
      def toKeyValue(k: K)(implicit configuration: Configuration) = (convK.toWritable(k), NullWritable.get)
    }
    new SeqSink[convK.SeqType, NullWritable, K](path, keyClass, valueClass, converter, overwrite)
  }

  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "value" component in a Sequence File. */
  def convertValueToSequenceFile[V](dl: DList[V], path: String, overwrite: Boolean = false)(implicit convV: SeqSchema[V]) =
    dl.addSink(valueSchemaSequenceFile(path, overwrite))

  def valueSchemaSequenceFile[V](path: String, overwrite: Boolean = false)(implicit convV: SeqSchema[V]) = {
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.SeqType]]
    val converter = new OutputConverter[NullWritable, convV.SeqType, V] {
      def toKeyValue(v: V)(implicit configuration: Configuration) = (NullWritable.get, convV.toWritable(v))
    }
    new ValueSeqSink[convV.SeqType, V](path, valueClass, converter, overwrite)(convV)
  }

  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as "key-values" in a Sequence File. */
  def convertToSequenceFile[K, V](dl: DList[(K, V)], path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K], convV: SeqSchema[V]) =
    dl.addSink(schemaSequenceSink(path, overwrite)(convK, convV))

  def schemaSequenceSink[K, V](path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K], convV: SeqSchema[V])= {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.SeqType]]
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.SeqType]]

    val converter = new OutputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }
    new SeqSink[convK.SeqType, convV.SeqType, (K, V)](path, keyClass, valueClass, converter, overwrite)
  }

  /** Specify a distributed list to be persistent by storing it to disk as a Sequence File. */
  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: DList[(K, V)], path: String, overwrite: Boolean = false) =
    dl.addSink(sequenceSink[K, V](path, overwrite))

  def sequenceSink[K <: Writable : Manifest, V <: Writable : Manifest](path: String, overwrite: Boolean = false) = {
    val keyClass = implicitly[Manifest[K]].erasure.asInstanceOf[Class[K]]
    val valueClass = implicitly[Manifest[V]].erasure.asInstanceOf[Class[V]]

    val converter = new OutputConverter[K, V, (K, V)] {
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (kv._1, kv._2)
    }
    new SeqSink[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite)
  }

  class ValueSeqSink[V, B : SeqSchema](path: String, valueClass: Class[V], converter: OutputConverter[NullWritable, V, B], overwrite: Boolean) extends
    SeqSink[NullWritable, V, B](path, classOf[NullWritable], valueClass, converter, overwrite) {

    override def toSource = Some(SequenceInput.valueSource(Seq(path))(implicitly[SeqSchema[B]]))
  }

    /* Class that abstracts all the common functionality of persisting to sequence files. */
  class SeqSink[K, V, B](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      converter: OutputConverter[K, V, B],
      overwrite: Boolean)
    extends DataSink[K, V, B] {

    protected val output = new Path(path)

    def outputFormat(implicit sc: ScoobiConfiguration) = classOf[SequenceFileOutputFormat[K, V]]
    def outputKeyClass(implicit sc: ScoobiConfiguration) = keyClass
    def outputValueClass(implicit sc: ScoobiConfiguration) = valueClass

    def outputCheck(implicit sc: ScoobiConfiguration) {
      if (Helper.pathExists(output)(sc) && !overwrite) {
          throw new FileAlreadyExistsException("Output path already exists: " + output)
      } else logger.info("Output path: " + output.toUri.toASCIIString)
    }
    def outputPath(implicit sc: ScoobiConfiguration) = Some(output)

    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}

    override def outputSetup(implicit configuration: Configuration) {
      if (Helper.pathExists(output)(configuration) && overwrite) {
        logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
        Helper.deletePath(output)(configuration)
      }
    }

    lazy val outputConverter = converter

    override def toSource: Option[Source] = Some(SequenceInput.source(Seq(path)))
  }
}
