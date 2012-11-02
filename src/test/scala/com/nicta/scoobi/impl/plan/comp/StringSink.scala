package com.nicta.scoobi
package impl
package plan
package comp

import com.nicta.scoobi.core._
import org.apache.hadoop.mapreduce._

case class StringSink() extends DataSink[String, String, (String, String)] {
  def outputFormat: Class[_ <: OutputFormat[String, String]] = classOf[StringOutputFormat]
  def outputKeyClass: Class[String]   = classOf[String]
  def outputValueClass: Class[String] = classOf[String]
  def outputCheck(implicit sc: ScoobiConfiguration) {}
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}
  def outputConverter: OutputConverter[String, String, (String, String)] = StringOutputConverter()
}

case class StringOutputConverter() extends OutputConverter[String, String, (String, String)] {
  def toKeyValue(x: (String, String)): (String, String) = x
}

case class StringOutputFormat() extends OutputFormat[String, String] {
  def getRecordWriter(context: TaskAttemptContext): RecordWriter[String, String] = StringRecordWriter()
  def checkOutputSpecs(context: JobContext) {}
  def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = NoOutputCommitter()
}

case class StringRecordWriter() extends RecordWriter[String, String] {
  def write(key: String, value: String) {}
  def close(context: TaskAttemptContext) {}
}

case class NoOutputCommitter() extends OutputCommitter {
  def setupJob(jobContext: JobContext) {}
  def setupTask      (taskContext: TaskAttemptContext) {}
  def needsTaskCommit(taskContext: TaskAttemptContext) = false
  def commitTask     (taskContext: TaskAttemptContext) {}
  def abortTask      (taskContext: TaskAttemptContext) {}
}
