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
package impl
package plan
package source

import java.io.DataInput
import java.io.DataOutput
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import scala.collection.JavaConversions._

import core._
import impl._
import Configurations._
import impl.collection.Seqs._
import plan.DListImpl
import util.{DistCache}
import SeqInput._
import WireFormat._

/**
 * Function for creating a distributed lists from a scala.collection.Seq
 *
 * The code is very similar to the FunctionInput code but it was impossible to refactor due to the impossibility to
 * parameterize InputFormat with values (only classes can be declared in a DataSource)
 */
trait SeqInput {
  lazy val logger = LogFactory.getLog("scoobi.SeqInput")

  /** Create a distributed list of a specified length whose elements are coming from a scala collection */
  def fromSeq[A : WireFormat](seq: Seq[A]): DList[A] = {

    val source = new DataSource[NullWritable, Array[Byte], Array[Byte]] {

      val inputFormat = classOf[SeqInputFormat[Array[Byte]]]
      override def toString = "SeqInput("+id+")"

      def inputCheck(implicit sc: ScoobiConfiguration) {}

      def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
        job.getConfiguration.setInt(LengthProperty, seq.size)
        /* Because SeqInputFormat is shared between multiple instances of the Seq
         * DataSource, each must have a unique id to distinguish their serialised
         * elements that are pushed out by the distributed cache.
         * Note that the previous seq Id might have been set on a key such as "scoobi.input0:scoobi.seq.id"
         * This is why we need to look for keys by regular expression in order to find the maximum value to increment
         */
        val id = job.getConfiguration.incrementRegex(IdProperty, ".*"+IdProperty)
        DistCache.pushObject(job.getConfiguration, seq.map(toByteArray(_, implicitly[WireFormat[A]].toWire(_: A, _: DataOutput))), seqProperty(id))
      }

      def inputSize(implicit sc: ScoobiConfiguration): Long = seq.size.toLong

      lazy val inputConverter = new InputConverter[NullWritable, Array[Byte], Array[Byte]] {
        def fromKeyValue(context: InputContext, k: NullWritable, v: Array[Byte]) = v
      }
    }
    DListImpl(source).map(a => fromByteArray[A](a))
  }

  private def fromByteArray[A](barr: Array[Byte])(implicit wf: WireFormat[A]): A = {
    wf.fromWire(new ObjectInputStream(new ByteArrayInputStream(barr)))
  }
}

/** InputFormat for producing values based on a sequence. */
class SeqInputFormat[A] extends InputFormat[NullWritable, A] {
  lazy val logger = LogFactory.getLog("scoobi.SeqInput")

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, A] =
    new SeqRecordReader[A](split.asInstanceOf[SeqInputSplit[A]])

  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = context.getConfiguration
    val n = context.getConfiguration.getInt(LengthProperty, 0)
    val id = context.getConfiguration.getInt(IdProperty, 0)

    val seq = DistCache.pullObject[Seq[A]](context.getConfiguration, seqProperty(id)).getOrElse({sys.error("no seq found in the distributed cache for: "+seqProperty(id)); Seq()})

    val numSplitsHint = conf.getInt("mapred.map.tasks", 1)
    val splitSize = n / numSplitsHint

    logger.debug("id=" + id)
    logger.debug("n=" + n)
    logger.debug("numSplitsHint=" + numSplitsHint)
    logger.debug("splitSize=" + splitSize)

    split(seq, splitSize, (offset: Int, length: Int, ss: Seq[A]) => new SeqInputSplit(offset, length, ss))
  }
}

/** InputSplit for a range of values produced by a sequence. */
class SeqInputSplit[A](var start: Int, var length: Int, var seq: Seq[A]) extends InputSplit with Writable {
  def this() = this(0, 0, Seq())

  def getLength: Long = length.toLong

  def getLocations: Array[String] = new Array[String](0)

  def readFields(in: DataInput) {
    start = in.readInt()
    length = in.readInt()
    seq = readObject[Seq[A]](in)
  }

  def write(out: DataOutput) {
    out.writeInt(start)
    out.writeInt(length)
    writeObject(out, seq)
  }
}


/** RecordReader for producing sequences */
class SeqRecordReader[A](split: SeqInputSplit[A]) extends RecordReader[NullWritable, A] {

  private val end = split.start + split.length
  private var ix = split.start
  private var x: A = _

  def initialize(split: InputSplit, context: TaskAttemptContext) = {}
  def getCurrentKey(): NullWritable = NullWritable.get
  def getCurrentValue(): A = x
  def getProgress(): Float = (ix - (end - split.length)) / split.length

  def nextKeyValue(): Boolean = {
    if (ix < end) {
      x = split.seq(ix)
      ix += 1
      true
    } else {
      false
    }
  }

  def close() {}
}

object SeqInput extends SeqInput {
  /** Configuration property names. */
  val PropertyPrefix = "scoobi.seq"
  val LengthProperty = PropertyPrefix + ".n"
  val IdProperty = PropertyPrefix + ".id"
  def seqProperty(id: Int) = PropertyPrefix + ".seq" + id

  /**
   * write an object to a DataOutput, using an ObjectOutputStream
   */
  def writeObject[A](out: DataOutput, a: A) {
    val arr = toByteArray(a)
    out.writeInt(arr.size)
    out.write(arr)
  }

  def toByteArray[A](a: A, write: (A, ObjectOutputStream) => Unit = (x: A, out: ObjectOutputStream) => out.writeObject(x)): Array[Byte] = {
    val bytesOut = new ByteArrayOutputStream
    val bOut =  new ObjectOutputStream(bytesOut)
    write(a, bOut)
    bOut.close()
    bytesOut.toByteArray
  }

  /**
   * read an object from a DataInput, using an ObjectInputStream
   */
  def readObject[A](in: DataInput): A = {
    val size = in.readInt()
    val barr = new Array[Byte](size)
    in.readFully(barr)
    val bIn = new ObjectInputStream(new ByteArrayInputStream(barr))
    bIn.readObject.asInstanceOf[A]
  }
}