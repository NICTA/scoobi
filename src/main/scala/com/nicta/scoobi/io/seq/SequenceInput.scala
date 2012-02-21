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

import java.io.IOException
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.AST


/** Smart functions for materializing distributed lists by loading Sequence files. */
object SequenceInput {
  lazy val logger = LogFactory.getLog("scoobi.SequenceInput")


  /** Type class for conversion of Hadoop Writable types ot basic Scala types. */
  trait Conv[To] extends Serializable {
    type From <: Writable
    def fromWritable(x: From): To
  }

  /* Implicits for Conv type class. */
  implicit def BoolConv = new Conv[Boolean] {
    type From = BooleanWritable
    def fromWritable(x: BooleanWritable): Boolean = x.get
  }

  implicit def IntConv = new Conv[Int] {
    type From = IntWritable
    def fromWritable(x: IntWritable): Int = x.get
  }

  implicit def FloatConv = new Conv[Float] {
    type From = FloatWritable
    def fromWritable(x: FloatWritable): Float = x.get
  }

  implicit def LongConv = new Conv[Long] {
    type From = LongWritable
    def fromWritable(x: LongWritable): Long = x.get
  }

  implicit def DoubleConv = new Conv[Double] {
    type From = DoubleWritable
    def fromWritable(x: DoubleWritable): Double = x.get
  }

  implicit def StringConv = new Conv[String] {
    type From = Text
    def fromWritable(x: Text): String = x.toString
  }


  /** Create a new DList from the "key" contents of one or more Sequence Files. Note that the type parameter K
    * is the "converted" Scala type for the Writable key type that must be contained in the the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertKeyFromSequenceFile[K : Manifest : WireFormat : Conv](paths: String*): DList[K] =
    convertKeyFromSequenceFile(List(paths: _*))


  /** Create a new DList from the "key" contents of a list of one or more Sequence Files. Note that the type parameter
    * K is the "converted" Scala type for the Writable key type that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertKeyFromSequenceFile[K : Manifest : WireFormat : Conv](paths: List[String]): DList[K] = {
    val convK = implicitly[Conv[K]]

    val converter = new InputConverter[convK.From, NullWritable, K] {
      def fromKeyValue(k: convK.From, v: NullWritable) = convK.fromWritable(k)
    }

    new DList(Smart.Load(new SeqLoader[convK.From, NullWritable, K](paths, converter)))
  }


  /** Create a new DList from the "value" contents of one or more Sequence Files. Note that the type parameter V
    * is the "converted" Scala type for the Writable value type that must be contained in the the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertValueFromSequenceFile[V : Manifest : WireFormat : Conv](paths: String*): DList[V] =
    convertValueFromSequenceFile(List(paths: _*))


  /** Create a new DList from the "value" contents of a list of one or more Sequence Files. Note that the type parameter
    * V is the "converted" Scala type for the Writable value type that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertValueFromSequenceFile[V : Manifest : WireFormat : Conv](paths: List[String]): DList[V] = {
    val convV = implicitly[Conv[V]]

    val converter = new InputConverter[NullWritable, convV.From, V] {
      def fromKeyValue(k: NullWritable, v: convV.From) = convV.fromWritable(v)
    }

    new DList(Smart.Load(new SeqLoader[NullWritable, convV.From, V](paths, converter)))
  }


  /** Create a new DList from the contents of one or more Sequence Files. Note that the type parameters K and V
    * are the "converted" Scala types for the Writable key-value types that must be contained in the the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertFromSequenceFile[K : Manifest : WireFormat : Conv, V : Manifest : WireFormat : Conv](paths: String*): DList[(K, V)] =
    convertFromSequenceFile(List(paths: _*))


  /** Create a new DList from the contents of a list of one or more Sequence Files. Note that the type parameters
    * K and V are the "converted" Scala types for the Writable key-value types that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def convertFromSequenceFile[K : Manifest : WireFormat : Conv, V : Manifest : WireFormat : Conv](paths: List[String]): DList[(K, V)] = {

    val convK = implicitly[Conv[K]]
    val convV = implicitly[Conv[V]]

    val converter = new InputConverter[convK.From, convV.From, (K, V)] {
      def fromKeyValue(k: convK.From, v: convV.From) = (convK.fromWritable(k), convV.fromWritable(v))
    }

    new DList(Smart.Load(new SeqLoader[convK.From, convV.From, (K, V)](paths, converter)))
  }


  /** Create a new DList from the contents of one or more Sequence Files. Note that the type parameters K and V
    * must match the type key-value type of the Sequence Files. In the case of a directory being specified,
    * the input forms all the files in that directory. */
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: String*): DList[(K, V)] =
    fromSequenceFile(List(paths: _*))


  /** Create a new DList from the contents of a list of one or more Sequence Files. Note
    * that the type parameters K and V must match the type key-value type of the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in
    * that directory. */
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: List[String]): DList[(K, V)] = {
    val converter = new InputConverter[K, V, (K, V)] {
      def fromKeyValue(k: K, v: V) = (k, v)
    }

    new DList(Smart.Load(new SeqLoader[K, V, (K, V)](paths, converter)))
  }


  /* Class that abstracts all the common functionality of reading from sequence files. */
  private class SeqLoader[K, V, A : Manifest : WireFormat](paths: List[String], converter: InputConverter[K, V, A]) extends Loader[A] {
    def mkInputStore(node: AST.Load[A]) = new InputStore[K, V, A](node) {
      private val inputPaths = paths.map(p => new Path(p))

      val inputFormat = classOf[SequenceFileInputFormat[K, V]]

      def inputCheck() = inputPaths foreach { p =>
        if (Helper.pathExists(p))
          logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)) + ")")
        else
           throw new IOException("Input path" + p + " does not exist.")
      }

      def inputConfigure(job: Job) = inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }

      def inputSize(): Long = inputPaths.map(p => Helper.pathSize(p)).sum

      val inputConverter = converter
    }
  }
}
