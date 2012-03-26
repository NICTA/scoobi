/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.io.seq

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.DList
import com.nicta.scoobi.DListPersister
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.AST

trait SequenceOutputConversions {
  /** Type class for conversions of basic Scala types to Hadoop Writable types. */
  trait OutConv[From] {
    type To <: Writable
    def toWritable(x: From): To
    val mf: Manifest[To]
  }

  /* Implicits for Conv type class. */
  implicit def OutBoolConv = new OutConv[Boolean] {
    type To = BooleanWritable
    def toWritable(x: Boolean) = new BooleanWritable(x)
    val mf: Manifest[To] = implicitly
  }

  implicit def OutIntConv = new OutConv[Int] {
    type To = IntWritable
    def toWritable(x: Int) = new IntWritable(x)
    val mf: Manifest[To] = implicitly
  }

  implicit def OutFloatConv = new OutConv[Float] {
    type To = FloatWritable
    def toWritable(x: Float) = new FloatWritable(x)
    val mf: Manifest[To] = implicitly
  }

  implicit def OutLongConv = new OutConv[Long] {
    type To = LongWritable
    def toWritable(x: Long) = new LongWritable(x)
    val mf: Manifest[To] = implicitly
  }

  implicit def OutDoubleConv = new OutConv[Double] {
    type To = DoubleWritable
    def toWritable(x: Double) = new DoubleWritable(x)
    val mf: Manifest[To] = implicitly
  }

  implicit def OutStringConv = new OutConv[String] {
    type To = Text
    def toWritable(x: String) = new Text(x)
    val mf: Manifest[To] = implicitly
  }
}


/** Smart functions for persisting distributed lists by storing them as Sequence files. */
object SequenceOutput extends SequenceOutputConversions {
  lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "key" component in a Sequence File. */
  def convertKeyToSequenceFile[K](dl: DList[K], path: String)(implicit convK: OutConv[K]): DListPersister[K] = {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.To]]
    val valueClass = classOf[NullWritable]

    val converter = new OutputConverter[convK.To, NullWritable, K] {
      def toKeyValue(k: K) = (convK.toWritable(k), NullWritable.get)
    }

    new DListPersister(dl, new SeqPersister[convK.To, NullWritable, K](path, keyClass, valueClass, converter))
  }


  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as the "value" component in a Sequence File. */
  def convertValueToSequenceFile[V](dl: DList[V], path: String)(implicit convV: OutConv[V]): DListPersister[V] = {

    val keyClass = classOf[NullWritable]
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.To]]

    val converter = new OutputConverter[NullWritable, convV.To, V] {
      def toKeyValue(v: V) = (NullWritable.get, convV.toWritable(v))
    }

    new DListPersister(dl, new SeqPersister[NullWritable, convV.To, V](path, keyClass, valueClass, converter))
  }


  /** Specify a distributed list to be persistent by converting its elements to Writables and storing it
    * to disk as "key-values" in a Sequence File. */
  def convertToSequenceFile[K, V](dl: DList[(K, V)], path: String, overwrite:Boolean = false)(implicit convK: OutConv[K], convV: OutConv[V]): DListPersister[(K, V)] = {

    val keyClass = convK.mf.erasure.asInstanceOf[Class[convK.To]]
    val valueClass = convV.mf.erasure.asInstanceOf[Class[convV.To]]

    val converter = new OutputConverter[convK.To, convV.To, (K, V)] {
      def toKeyValue(kv: (K, V)) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }

    new DListPersister(dl, new SeqPersister[convK.To, convV.To, (K, V)](path, keyClass, valueClass, converter, overwrite))
  }


  /** Specify a distributed list to be persistent by storing it to disk as a Sequence File. */
  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: DList[(K, V)], path: String, overwrite:Boolean = false): DListPersister[(K, V)] = {

    val keyClass = implicitly[Manifest[K]].erasure.asInstanceOf[Class[K]]
    val valueClass = implicitly[Manifest[V]].erasure.asInstanceOf[Class[V]]

    val converter = new OutputConverter[K, V, (K, V)] {
      def toKeyValue(kv: (K, V)) = (kv._1, kv._2)
    }

    new DListPersister(dl, new SeqPersister[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite))
  }


  /* Class that abstracts all the common functionality of persisting to sequence files. */
  private class SeqPersister[K, V, B](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      converter: OutputConverter[K, V, B],
      overwrite: Boolean = false)
    extends Persister[B] {

    def mkOutputStore(node: AST.Node[B]) = new OutputStore[K, V, B](node) {
      protected val outputPath = new Path(path)

      val outputFormat = classOf[SequenceFileOutputFormat[K, V]]
      val outputKeyClass = keyClass
      val outputValueClass = valueClass

      def outputCheck() =
        if (Helper.pathExists(outputPath))
            if (overwrite){
              logger.info("Delete the existed output path:" + outputPath.toUri.toASCIIString)
              Helper.deletePath(outputPath)
            }else 
              throw new FileAlreadyExistsException("Output path already exists: " + outputPath)
        else
          logger.info("Output path: " + outputPath.toUri.toASCIIString)

      def outputConfigure(job: Job) = FileOutputFormat.setOutputPath(job, outputPath)

      val outputConverter = converter
    }
  }
}
