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

import java.util.Arrays._
import org.apache.hadoop.mapreduce._
import lib.input.InvalidInputException
import org.apache.hadoop.filecache.DistributedCache

import core._
import testing.TestFiles
import org.apache.hadoop.io.Writable
import java.io.{File, DataOutput, DataInput}

class ConstantStringDataSource(val value: String) extends DataSource[String, String, String] {

  override def toString = "ConstantString("+id+")"

  def inputFormat: Class[_ <: InputFormat[String, String]] = classOf[ConstantStringInputFormat]
  def inputCheck(implicit sc: ScoobiConfiguration) {}
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    job.getConfiguration.set("mapred.constant.string", "value")
  }
  def inputSize(implicit sc: ScoobiConfiguration): Long = value.size
  lazy val inputConverter = ConstantStringInputConverter(value)

  case class ConstantStringInputConverter(value: String) extends InputConverter[String, String, String] {
    def fromKeyValue(context: this.type#InputContext, key: String, v: String) = value
  }

  override def equals(a: Any) = a match {
    case other: ConstantStringDataSource => value == other.value
    case _                               => false
  }
}
case class ConstantStringInputSplit(var value: String) extends InputSplit with Writable {
  def this() = this("")
  def getLength = value.size
  def readFields(in: DataInput) {}

  def write(out: DataOutput) { out.writeChars(value) }
  def getLocations = Array("localhost")


}


object ConstantStringDataSource {
  def apply(value: String) = new ConstantStringDataSource(value)
}
class FailingDataSource extends ConstantStringDataSource("") {
  override def inputFormat: Class[_ <: InputFormat[String, String]] = classOf[FailingInputFormat]
}
object FailingDataSource {
  def apply() = new FailingDataSource
}
case class ConstantStringRecordReader(value: String) extends RecordReader[String, String] {
  private var read = false
  def this() = this("value")
  def initialize(split: InputSplit, context: TaskAttemptContext) { read = false }
  def nextKeyValue() = { if (read) false else { read = true; true } }
  def getCurrentKey = value
  def getCurrentValue = value
  def getProgress = 0.0f
  def close() { read = false }
}

class ConstantStringInputFormat(value: String) extends InputFormat[String, String] {
  def this() = this("value")
  def getSplits(context: JobContext) = asList(ConstantStringInputSplit(value))
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = ConstantStringRecordReader(value)
}

class FailingInputFormat extends InputFormat[String, String] {
  def getSplits(context: JobContext) = { throw new InvalidInputException(asList()); asList() }
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = ConstantStringRecordReader("")
}

