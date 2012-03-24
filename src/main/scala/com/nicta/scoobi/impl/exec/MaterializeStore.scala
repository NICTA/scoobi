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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.util.UniqueInt
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.rtt.RuntimeClass


/** A MaterializeStore is a DataSink for eventual consumption by a Scala program. It is used
  * in conjunction with 'materialize'. */
final case class MaterializeStore[A : Manifest : WireFormat](n: AST.Node[A], id: Int, path: Path)
  extends OutputStore[NullWritable, ScoobiWritable[A], A](n) {

  lazy val logger = LogFactory.getLog("scoobi.Materialize")
  private val typeName = "MS" + id

  val rtClass = ScoobiWritable(typeName, implicitly[Manifest[A]], implicitly[WireFormat[A]])

  val outputFormat = classOf[SequenceFileOutputFormat[NullWritable, ScoobiWritable[A]]]
  val outputKeyClass = classOf[NullWritable]
  def outputValueClass = rtClass.clazz.asInstanceOf[Class[ScoobiWritable[A]]]
  def outputCheck() = {}
  def outputConfigure(job: Job) = FileOutputFormat.setOutputPath(job, path)
  val outputConverter = new ScoobiWritableOutputConverter[A](typeName)
}

object MaterializeId extends UniqueInt
