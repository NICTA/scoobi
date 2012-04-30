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
package com.nicta.scoobi.impl.exec

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.util.UniqueInt
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.rtt.RuntimeClass


/** A bridge store is any data that moves between MSCRs. It must first be computed, but
  * may be removed once all successor MSCRs have consumed it. */
final case class BridgeStore[A]()
  extends DataSource[NullWritable, ScoobiWritable[A], A]
  with DataSink[NullWritable, ScoobiWritable[A], A] {

  lazy val logger = LogFactory.getLog("scoobi.Bridge")

  private val id = BridgeId.get
  private val path = new Path(Scoobi.getWorkingDirectory(Scoobi.conf), "bridges/" + id)
  val typeName = "BS" + id

  /* rtClass will be created at runtime as part of building the MapReduce job. */
  var rtClass: Option[RuntimeClass] = None


  /* Output (i.e. input to bridge) */
  val outputFormat = classOf[SequenceFileOutputFormat[NullWritable, ScoobiWritable[A]]]
  val outputKeyClass = classOf[NullWritable]
  def outputValueClass = rtClass.orNull.clazz.asInstanceOf[Class[ScoobiWritable[A]]]
  def outputCheck() = {}
  def outputConfigure(job: Job) = FileOutputFormat.setOutputPath(job, path)
  val outputConverter = new ScoobiWritableOutputConverter[A](typeName)


  /* Input (i.e. output of bridge) */
  val inputFormat = classOf[SequenceFileInputFormat[NullWritable, ScoobiWritable[A]]]
  def inputCheck() = {}
  def inputConfigure(job: Job) = FileInputFormat.addInputPath(job, new Path(path, "ch*"))
  def inputSize(): Long = Helper.pathSize(new Path(path, "ch*"))
  val inputConverter = new InputConverter[NullWritable, ScoobiWritable[A], A] {
    def fromKeyValue(context: InputContext, key: NullWritable, value: ScoobiWritable[A]): A = value.get
  }


  /* Free up the disk space being taken up by this intermediate data. */
  def freePath: Unit = {
    val fs = path.getFileSystem(Scoobi.conf)
    fs.delete(path, true)
  }
}

object BridgeId extends UniqueInt

/** OutputConverter for a bridges. The expectation is that by the time toKeyValue is called,
  * the Class for 'value' will exist and be known by the ClassLoader. */
class ScoobiWritableOutputConverter[A](typeName: String) extends OutputConverter[NullWritable, ScoobiWritable[A], A] {
  lazy val value: ScoobiWritable[A] = Class.forName(typeName).newInstance.asInstanceOf[ScoobiWritable[A]]
  def toKeyValue(x: A): (NullWritable, ScoobiWritable[A]) = {
    value.set(x)
    (NullWritable.get, value)
  }
}
