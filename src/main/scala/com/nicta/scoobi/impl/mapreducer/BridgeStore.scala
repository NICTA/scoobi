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
package mapreducer

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.{Writable, NullWritable, SequenceFile}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job

import core._
import rtt._
import io.Helper
import ScoobiConfiguration._
import org.apache.hadoop.conf.Configuration

/** A bridge store is any data that moves between MSCRs. It must first be computed, but
  * may be removed once all successor MSCRs have consumed it. */
case class BridgeStore[A](bridgeStoreId: String, wf: WireReaderWriter)
  extends DataSource[NullWritable, ScoobiWritable[A], A]
  with DataSink[NullWritable, ScoobiWritable[A], A] with Bridge {

  override lazy val id: Int = Data.ids.get
  override lazy val stringId = bridgeStoreId

  lazy val logger = LogFactory.getLog("scoobi.Bridge")

  /** rtClass will be created at runtime as part of building the MapReduce job. */
  def rtClass(implicit sc: ScoobiConfiguration): RuntimeClass = ScoobiWritable(typeName, wf)

  /** type of the generated class for this Bridge */
  lazy val typeName = "BS" + bridgeStoreId

  def path(implicit sc: ScoobiConfiguration) = new Path(sc.workingDirectory, "bridges/" + bridgeStoreId)

  /* Output (i.e. input to bridge) */
  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[SequenceFileOutputFormat[NullWritable, ScoobiWritable[A]]]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]
  def outputValueClass(implicit sc: ScoobiConfiguration) = rtClass(sc).clazz.asInstanceOf[Class[ScoobiWritable[A]]]
  def outputCheck(implicit sc: ScoobiConfiguration) {}
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}
  def outputPath(implicit sc: ScoobiConfiguration) = Some(path)

  lazy val outputConverter = new ScoobiWritableOutputConverter[A](typeName)


  /* Input (i.e. output of bridge) */
  lazy val inputFormat = classOf[SequenceFileInputFormat[NullWritable, ScoobiWritable[A]]]
  def inputCheck(implicit sc: ScoobiConfiguration) {}
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
     FileInputFormat.addInputPath(job, new Path(path(sc), "ch*"))
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = Helper.pathSize(new Path(path, "ch*"))(sc)

  lazy val inputConverter = new InputConverter[NullWritable, ScoobiWritable[A], A] {
    def fromKeyValue(context: InputContext, key: NullWritable, value: ScoobiWritable[A]): A = value.get
  }


  /* Free up the disk space being taken up by this intermediate data. */
  def freePath(implicit sc: ScoobiConfiguration) {
    val fs = path.getFileSystem(sc)
    fs.delete(path, true)
  }


  /**
   * Read the contents of this bridge store sequence files as an Iterable collection. The
   * underlying Iterator has a lazy implementation and will only bring one element into memory
   * at a time
   */
  def readAsIterable(implicit sc: ScoobiConfiguration): Iterable[A] = new Iterable[A] {
    /** instantiate a ScoobiWritable from the Writable class generated for this BridgeStore */
    lazy val value: ScoobiWritable[A] =
      rtClass(sc).clazz.newInstance.asInstanceOf[ScoobiWritable[A]]

    def iterator = new BridgeStoreIterator[A](value, path, sc)
  }


  override def toString = typeName+"("+id+")"

  override def equals(other: Any) = {
    other match {
      case bs: BridgeStore[_] => bs.bridgeStoreId == this.bridgeStoreId
      case _                  => false
    }
  }

  override def hashCode = bridgeStoreId.hashCode

  override def toSource: Option[Source] = Some(this)
}

class BridgeStoreIterator[A](value: ScoobiWritable[A], path: Path, sc: ScoobiConfiguration) extends Iterator[A] {
  def fs = FileSystem.get(path.toUri, sc)

  private var initialised = false
  def init {
    if (!initialised)  {
      readers = fs.globStatus(new Path(path, "ch*")) map { (stat: FileStatus) =>
        new SequenceFile.Reader(sc, SequenceFile.Reader.file(stat.getPath))
      }
      remainingReaders = readers.toList
      empty = readers.isEmpty || !readNext()
      initialised = true
    }
  }
  private var readers: Seq[SequenceFile.Reader] = _
  private var remainingReaders: List[SequenceFile.Reader] = _
  private var empty: Boolean = _
  private val key = NullWritable.get

  def next(): A = {
    init
    val v = value.get
    empty = !readNext()
    v
  }
  def hasNext(): Boolean = {
    init
    !empty
  }

  /* Attempt to read the next key-value and return true if successful, else false. As the
   * end of each SequenceFile.Reader is reached, move on to the next until they have all
   * been read. */
  private def readNext(): Boolean = {
    remainingReaders match {
      case cur :: rest =>
        val nextValueIsRead = try { cur.next(key, value) } catch { case e: Throwable => e.printStackTrace; close; false }
        nextValueIsRead || { cur.close(); remainingReaders = rest; readNext() }
      case Nil         => false
    }
  }

  def close {
    Option(remainingReaders).map(rs => rs.foreach(_.close))
  }
}

/** OutputConverter for a bridges. The expectation is that by the time toKeyValue is called,
  * the Class for 'value' will exist and be known by the ClassLoader. */
class ScoobiWritableOutputConverter[A](typeName: String) extends OutputConverter[NullWritable, ScoobiWritable[A], A] {
  lazy val value: ScoobiWritable[A] = Class.forName(typeName).newInstance.asInstanceOf[ScoobiWritable[A]]
  def toKeyValue(x: A)(implicit configuration: Configuration): (NullWritable, ScoobiWritable[A]) = {
    value.configuration = configuration
    value.set(x)
    (NullWritable.get, value)
  }
}
