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

import application.DListPersister
import core._
import application.ScoobiConfiguration

/** Smart functions for persisting distributed lists by storing them as Sequence files. */
object SequenceOutput {
  lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "key" component in a Sequence File. */
  def convertKeyToSequenceFile[K](dl: DList[K], path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K]): DListPersister[K] = {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.SeqType]]
    val valueClass = classOf[NullWritable]

    val converter = new OutputConverter[convK.SeqType, NullWritable, K] {
      def toKeyValue(k: K) = (convK.toWritable(k), NullWritable.get)
    }

    new DListPersister(dl, new SeqSink[convK.SeqType, NullWritable, K](path, keyClass, valueClass, converter, overwrite))
  }


  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "value" component in a Sequence File. */
  def convertValueToSequenceFile[V](dl: DList[V], path: String, overwrite: Boolean = false)(implicit convV: SeqSchema[V]): DListPersister[V] = {

    val keyClass = classOf[NullWritable]
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.SeqType]]

    val converter = new OutputConverter[NullWritable, convV.SeqType, V] {
      def toKeyValue(v: V) = (NullWritable.get, convV.toWritable(v))
    }

    new DListPersister(dl, new SeqSink[NullWritable, convV.SeqType, V](path, keyClass, valueClass, converter, overwrite))
  }


  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as "key-values" in a Sequence File. */
  def convertToSequenceFile[K, V](dl: DList[(K, V)], path: String, overwrite: Boolean = false)(implicit convK: SeqSchema[K], convV: SeqSchema[V]): DListPersister[(K, V)] = {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.SeqType]]
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.SeqType]]

    val converter = new OutputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def toKeyValue(kv: (K, V)) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }

    new DListPersister(dl, new SeqSink[convK.SeqType, convV.SeqType, (K, V)](path, keyClass, valueClass, converter, overwrite))
  }


  /** Specify a distributed list to be persistent by storing it to disk as a Sequence File. */
  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: DList[(K, V)], path: String, overwrite: Boolean = false): DListPersister[(K, V)] = {

    val keyClass = implicitly[Manifest[K]].erasure.asInstanceOf[Class[K]]
    val valueClass = implicitly[Manifest[V]].erasure.asInstanceOf[Class[V]]

    val converter = new OutputConverter[K, V, (K, V)] {
      def toKeyValue(kv: (K, V)) = (kv._1, kv._2)
    }

    new DListPersister(dl, new SeqSink[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite))
  }


  /* Class that abstracts all the common functionality of persisting to sequence files. */
  private class SeqSink[K, V, B](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      converter: OutputConverter[K, V, B],
      overwrite: Boolean)
    extends DataSink[K, V, B] {

    protected val outputPath = new Path(path)

    val outputFormat = classOf[SequenceFileOutputFormat[K, V]]
    val outputKeyClass = keyClass
    val outputValueClass = valueClass

    def outputCheck(sc: ScoobiConfiguration) {
      if (Helper.pathExists(outputPath)(sc))
        if (overwrite) {
          logger.info("Deleting the pre-existing output path: " + outputPath.toUri.toASCIIString)
          Helper.deletePath(outputPath)(sc)
        } else {
          throw new FileAlreadyExistsException("Output path already exists: " + outputPath)
        }
      else
        logger.info("Output path: " + outputPath.toUri.toASCIIString)
    }

    def outputConfigure(job: Job) {
      FileOutputFormat.setOutputPath(job, outputPath)
    }

    lazy val outputConverter = converter
  }
}
