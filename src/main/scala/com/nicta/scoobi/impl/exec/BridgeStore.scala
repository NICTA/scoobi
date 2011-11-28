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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import scala.collection.mutable.{Map => MMap}

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.util.UniqueInt


/** A bridge store is any data that moves between MSCRs. It must first be computed, but
  * may be removed once all successor MSCRs have consumed it. */
final case class BridgeStore(n: AST.Node[_], val path: Path) extends DataStore(n) with DataSource with DataSink {

  def inputTypeName = typeName
  val inputPath = new Path(path, "ch*")
  val inputFormat = classOf[SequenceFileInputFormat[_,_]]

  def outputTypeName = typeName
  val outputPath = path
  val outputFormat = classOf[SequenceFileOutputFormat[_,_]]

  /** Free up the disk space being taken up by this intermediate data. */
  def freePath: Unit = {
    val fs = outputPath.getFileSystem(Scoobi.conf)
    fs.delete(outputPath, true)
  }
}

/** Companion object for automating the creation of random temporary paths as the
  * location for bridge stores. */
object BridgeStore {

  private object TmpId extends UniqueInt

  def apply(node: AST.Node[_]): BridgeStore = {
    val tmpPath = new Path(Scoobi.getWorkingDirectory(Scoobi.conf), "bridges/" + TmpId.get.toString)
    BridgeStore(node, tmpPath)
  }
}
