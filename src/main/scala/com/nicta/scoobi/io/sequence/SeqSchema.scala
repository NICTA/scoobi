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

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ByteWritable
import org.apache.hadoop.io.BytesWritable
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.collection.JavaConversions._


/** Type class for conversions between basic Scala types and Hadoop Writable types. */
trait SeqSchema[A] {
  type SeqType <: Writable
  def toWritable(x: A): SeqType
  def fromWritable(x: SeqType): A
  val mf: Manifest[SeqType]
}


/* Implicits instances for SeqSchema type class. */
object SeqSchema {
  implicit object BoolSchema extends SeqSchema[Boolean] {
    type SeqType = BooleanWritable
    def toWritable(x: Boolean) = new BooleanWritable(x)
    def fromWritable(x: BooleanWritable): Boolean = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object IntSchema extends SeqSchema[Int] {
    type SeqType = IntWritable
    def toWritable(x: Int) = new IntWritable(x)
    def fromWritable(x: IntWritable): Int = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object FloatSchema extends SeqSchema[Float] {
    type SeqType = FloatWritable
    def toWritable(x: Float) = new FloatWritable(x)
    def fromWritable(x: FloatWritable): Float = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object LongSchema extends SeqSchema[Long] {
    type SeqType = LongWritable
    def toWritable(x: Long) = new LongWritable(x)
    def fromWritable(x: LongWritable): Long = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object DoubleSchema extends SeqSchema[Double] {
    type SeqType = DoubleWritable
    def toWritable(x: Double) = new DoubleWritable(x)
    def fromWritable(x: DoubleWritable): Double = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object StringSchema extends SeqSchema[String] {
    type SeqType = Text
    def toWritable(x: String) = new Text(x)
    def fromWritable(x: Text): String = x.toString
    val mf: Manifest[SeqType] = implicitly
  }

  implicit object ByteSchema extends SeqSchema[Byte] {
    type SeqType = ByteWritable
    def toWritable(x: Byte) = new ByteWritable(x)
    def fromWritable(x: ByteWritable): Byte = x.get
    val mf: Manifest[SeqType] = implicitly
  }

  implicit def BytesConv[CC[X] <: Traversable[X]](implicit bf: CanBuildFrom[_, Byte, CC[Byte]]) = new SeqSchema[CC[Byte]] {
    type SeqType = BytesWritable
    val b: Builder[Byte, CC[Byte]] = bf()
    def toWritable(xs: CC[Byte]) = new BytesWritable(xs.toArray)
    def fromWritable(xs: BytesWritable): CC[Byte]= {
      b.clear()
      xs.getBytes.take(xs.getLength).foreach { x => b += x }
      b.result()
    }
    val mf: Manifest[SeqType] = implicitly
  }
}
