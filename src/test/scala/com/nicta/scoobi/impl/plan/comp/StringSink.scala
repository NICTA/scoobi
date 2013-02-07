package com.nicta.scoobi
package impl
package plan
package comp

import com.nicta.scoobi.core._
import org.apache.hadoop.mapreduce._
import scala.collection.mutable._

case class StringSink() extends DataSink[String, String, String] {
  def outputFormat(implicit sc: ScoobiConfiguration): Class[_ <: OutputFormat[String, String]] = classOf[StringOutputFormat]
  def outputKeyClass(implicit sc: ScoobiConfiguration): Class[String]   = classOf[String]
  def outputValueClass(implicit sc: ScoobiConfiguration): Class[String] = classOf[String]
  def outputCheck(implicit sc: ScoobiConfiguration) {}
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}
  def outputConverter: OutputConverter[String, String, String] = StringOutputConverter()
}

object StringSink {
  val results: MultiMap[String, String] = new HashMap[String, Set[String]] with MultiMap[String, String]
}
case class StringOutputConverter() extends OutputConverter[String, String, String] {
  def toKeyValue(x: String): (String, String) = ("nokey", x)
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
