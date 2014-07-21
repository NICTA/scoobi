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

import java.io._
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
import com.nicta.scoobi.impl.util.{Compatibility, Serialiser, DistCache}
import SeqInput._
import WireFormat._
import com.nicta.scoobi.impl.rtt.Configured

/**
 * Function for creating a distributed lists from a scala.collection.Seq
 *
 * The code is very similar to the FunctionInput code but it was impossible to refactor due to the impossibility to
 * parameterize InputFormat with values (only classes can be declared in a DataSource)
 */
trait SeqInput {
  lazy val logger = LogFactory.getLog("scoobi.SeqInput")

  /**
   * Create a distributed list of a specified length whose elements are coming from a scala collection
   */
  def fromSeq[A : WireFormat](seq: Seq[A]): DList[A] = {

    val source = new DataSource[NullWritable, A, A] {

      val inputFormat = classOf[SeqInputFormat[A]]
      override def toString = "SeqInput("+id+")"

      def inputCheck(implicit sc: ScoobiConfiguration) {}

      def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
        job.getConfiguration.setInt(LengthProperty, seq.size)
        job.getConfiguration.set(IdProperty, id.toString)

        val serialiser = (sequence: Seq[A], stream: DataOutputStream) =>
          implicitly[WireFormat[Seq[A]]].toWire(sequence, stream)

        job.getConfiguration.set(IdProperty, id.toString)
        DistCache.pushObject(job.getConfiguration, seq, serialiser, seqProperty(id))
        DistCache.pushObject(job.getConfiguration, implicitly[WireFormat[A]], wireFormatProperty(id))
      }

      def inputSize(implicit sc: ScoobiConfiguration): Long = seq.size.toLong

      lazy val inputConverter = new InputConverter[NullWritable, A, A] {
        def fromKeyValue(context: InputContext, k: NullWritable, v: A) = v
      }
    }
    DListImpl(source)
  }

  /**
   * Create a distributed list of a specified length whose elements are coming from a scala collection which will only be evaluated
   * when the source is read
   */
  def fromLazySeq[A : WireFormat](seq: () => Seq[A], seqSize: Int = 1000): DList[A] = {

    val source = new DataSource[NullWritable, A, A] {

      val inputFormat = classOf[LazySeqInputFormat[A]]
      override def toString = "LazySeqInput("+id+")"

      def inputCheck(implicit sc: ScoobiConfiguration) {}

      def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
        job.getConfiguration.set(IdProperty, id.toString)
        job.getConfiguration.set(SeqSizeProperty, seqSize.toString)
        DistCache.pushObject(job.getConfiguration, seq, seqProperty(id))
        DistCache.pushObject(job.getConfiguration, implicitly[WireFormat[A]], wireFormatProperty(id))
      }

      def inputSize(implicit sc: ScoobiConfiguration): Long = 1

      lazy val inputConverter = new InputConverter[NullWritable, A, A] {
        def fromKeyValue(context: InputContext, k: NullWritable, v: A) = v
      }
    }
    DListImpl(source)
  }
}

/** InputFormat for producing values based on a sequence. */
class SeqInputFormat[A] extends InputFormat[NullWritable, A] {
  lazy val logger = LogFactory.getLog("scoobi.SeqInput")

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, A] =
    new SeqRecordReader[A](split.asInstanceOf[SeqInputSplit[A]])

  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = Compatibility.getConfiguration(context)
    val n    = conf.getInt(LengthProperty, 0)
    val id   = conf.getInt(IdProperty, 0)

    implicit val wf  = DistCache.pullObject[WireFormat[A]](conf, wireFormatProperty(id)).getOrElse({sys.error("no wireformat found in the distributed cache for: "+wireFormatProperty(id)); null})
    val deserialiser = (stream: DataInputStream) => implicitly[WireFormat[Seq[A]]].fromWire(stream)
    val seq = DistCache.pullObjectDeserialise[Seq[A]](conf, seqProperty(id), deserialiser).getOrElse({sys.error("no seq found in the distributed cache for: "+seqProperty(id)); Seq()})

    val numSplitsHint = conf.getInt("mapred.map.tasks", 1)
    val splitSize = {
      val ss = n / numSplitsHint
      if (ss == 0) 1 else ss
    }

    logger.debug("id=" + id)
    logger.debug("n=" + n)
    logger.debug("numSplitsHint=" + numSplitsHint)
    logger.debug("splitSize=" + splitSize)

    split(seq, splitSize, (offset: Int, length: Int, ss: Seq[A]) => new SeqInputSplit(ss.drop(offset).take(length), wf, wireFormatProperty(id)))
  }
}

/** InputSplit for a range of values produced by a sequence. */
class SeqInputSplit[A](var seq: Seq[A], var wf: WireFormat[A], var wfTag: String) extends InputSplit with Writable with Configured {
  def this() = this(Seq(), null, "")
  def getLength: Long = seq.size.toLong

  def getLocations: Array[String] = new Array[String](0)

  def readFields(in: DataInput) {
    wfTag = in.readUTF()
    implicit val wfa = DistCache.pullObject[WireFormat[A]](configuration, wfTag).getOrElse({sys.error("no wireformat found in the distributed cache for: "+wfTag); null})
    implicit val wfs = implicitly[WireFormat[Seq[A]]]
    seq = wfs.read(in)
  }

  def write(out: DataOutput) {
    out.writeUTF(wfTag)
    implicit val wfa: WireFormat[A] = wf
    implicit val wfs = implicitly[WireFormat[Seq[A]]]
    wfs.write(seq, out)
  }
}


/** RecordReader for producing sequences */
class SeqRecordReader[A](split: SeqInputSplit[A]) extends RecordReader[NullWritable, A] {

  private val size = split.seq.size
  private var ix = 0
  private var x: A = _

  def initialize(split: InputSplit, context: TaskAttemptContext) = {}
  def getCurrentKey(): NullWritable = NullWritable.get
  def getCurrentValue(): A = x
  def getProgress(): Float = (size - ix) / size

  def nextKeyValue(): Boolean = {
    if (ix < size) {
      x = split.seq(ix)
      ix += 1
      true
    } else false
  }

  def close() {}
}

object SeqInput extends SeqInput {
  /** Configuration property names. */
  val PropertyPrefix = "scoobi.seq"
  val LengthProperty = PropertyPrefix + ".n"
  val IdProperty = PropertyPrefix + ".id"
  val SeqSizeProperty = PropertyPrefix + ".size"
  def seqProperty(id: Int) = PropertyPrefix + ".seq" + id
  def wireFormatProperty(id: Int) = PropertyPrefix + ".wf" + id

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

/** InputFormat for producing values based on a lazy sequence. */
class LazySeqInputFormat[A] extends InputFormat[NullWritable, A] {
  lazy val logger = LogFactory.getLog("scoobi.LazySeqInput")

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, A] =
    new SeqRecordReader[A](split.asInstanceOf[SeqInputSplit[A]])

  def getSplits(context: JobContext): java.util.List[InputSplit] = {
    val conf = Compatibility.getConfiguration(context)
    val n    = conf.getInt(SeqSizeProperty, 1)
    val id   = conf.getInt(IdProperty, 0)

    val seq = DistCache.pullObject[() => Seq[A]](conf, seqProperty(id)).getOrElse({sys.error("no seq found in the distributed cache for: "+seqProperty(id)); () => Seq()})
    val wf  = DistCache.pullObject[WireFormat[A]](conf, wireFormatProperty(id)).getOrElse({sys.error("no wireformat found in the distributed cache for: "+wireFormatProperty(id)); null})

    val numSplitsHint = conf.getInt("mapred.map.tasks", 1)
    val splitSize = n / numSplitsHint

    logger.debug("id=" + id)
    logger.debug("n=" + n)
    logger.debug("numSplitsHint=" + numSplitsHint)
    logger.debug("splitSize=" + splitSize)

    split(seq().toStream, splitSize, (offset: Int, length: Int, ss: Seq[A]) => new SeqInputSplit(ss.drop(offset).take(length), wf, wireFormatProperty(id)))
  }
}
