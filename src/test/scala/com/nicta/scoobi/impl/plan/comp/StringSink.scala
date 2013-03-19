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
package comp

import com.nicta.scoobi.core._
import org.apache.hadoop.mapreduce._
import scala.collection.mutable._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

case class StringSink() extends DataSink[String, String, String] {
  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[String, String]] = classOf[StringOutputFormat]
  def outputKeyClass(implicit sc: ScoobiConfiguration): Class[String]   = classOf[String]
  def outputValueClass(implicit sc: ScoobiConfiguration): Class[String] = classOf[String]
  def outputCheck(implicit sc: ScoobiConfiguration) {}
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}
  def outputPath(implicit sc: ScoobiConfiguration): Option[Path] = None
  def outputConverter: OutputConverter[String, String, String] = StringOutputConverter()
}

object StringSink {
  val results: MultiMap[String, String] = new HashMap[String, Set[String]] with MultiMap[String, String]
}
case class StringOutputConverter() extends OutputConverter[String, String, String] {
  def toKeyValue(x: String)(implicit configuration: Configuration): (String, String) = ("nokey", x)
}

case class StringOutputFormat() extends OutputFormat[String, String] {
  def getRecordWriter(context: TaskAttemptContext): RecordWriter[String, String] = StringRecordWriter()
  def checkOutputSpecs(context: JobContext) {}
  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = NoOutputCommitter()
}

case class StringRecordWriter() extends RecordWriter[String, String] {
  def write(key: String, value: String) { StringSink.results.addBinding(key, value) }
  def close(context: TaskAttemptContext) {}
}

case class NoOutputCommitter() extends OutputCommitter {
  def setupJob(jobContext: JobContext) {}
  def setupTask      (taskContext: TaskAttemptContext) {}
  def needsTaskCommit(taskContext: TaskAttemptContext) = false
  def commitTask     (taskContext: TaskAttemptContext) {}
  def abortTask      (taskContext: TaskAttemptContext) {}
}
