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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.filecache.DistributedCache._
import org.apache.hadoop.fs.Path
import scala.collection.mutable.{Map => MMap}

import core._
import Configurations._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * A class that simplifies writing output to different paths and with different types
 * depending on the output channel and required outputs per channel
 */
class ChannelOutputFormat(context: TaskInputOutputContext[_, _, _, _]) {

  private val taskContexts: MMap[(Int, Int), TaskAttemptContext] = MMap.empty
  private val recordWriters: MMap[(Int, Int), RecordWriter[_,_]] = MMap.empty

  /** write a value out on multiple outputs of a given output channel.*/
  def write[K, V](tag: Int, sinkId: Int, kv: (K, V)) {
    val taskContext = getContext(tag, sinkId)
    val recordWriter = getRecordWriter(taskContext, tag, sinkId).asInstanceOf[RecordWriter[K, V]]
    recordWriter.write(kv._1, kv._2)
  }

  /** Close all opened output channels. */
  def close() { recordWriters.values foreach { _.close(context) } }

  private def getContext(tag: Int, sinkId: Int): TaskAttemptContext = {
    /* The following trick leverages the instantiation of a record writer via
     * the job thus supporting arbitrary output formats. */
    def mkTaskContext = {
      val conf = context.getConfiguration
      val job = new Job(new Configuration(conf))

      /* Set standard properties. */
      val format = conf.getClass(ChannelOutputFormat.formatProperty(tag, sinkId), null)
                       .asInstanceOf[Class[_ <: OutputFormat[_,_]]]
      job.setOutputFormatClass(format)
      job.setOutputKeyClass(conf.getClass(ChannelOutputFormat.keyClassProperty(tag, sinkId), null))
      job.setOutputValueClass(conf.getClass(ChannelOutputFormat.valueClassProperty(tag, sinkId), null))
      job.getConfiguration.set("mapreduce.output.basename",  "ch" + tag + "out" + sinkId)
      job.getConfiguration.set("avro.mo.config.namedOutput", "ch" + tag + "out" + sinkId)

      val PropertyPrefix = (ChannelOutputFormat.otherProperty(tag, sinkId) + """(.*)""").r
      conf.toMap collect { case (PropertyPrefix(k), v) => (k, v) } foreach {
        case (k, v) => job.getConfiguration.set(k, v)
      }

      new TaskAttemptContextImpl(job.getConfiguration, context.getTaskAttemptID)
    }

    taskContexts.getOrElseUpdate((tag, sinkId), mkTaskContext)
   }

  private def getRecordWriter(taskContext: TaskAttemptContext, tag: Int, sinkId: Int): RecordWriter[_,_] = {

    /* Get the record writer from context output format. */
    def mkRecordWriter =
      ReflectionUtils.newInstance(taskContext.getOutputFormatClass, taskContext.getConfiguration)
                     .asInstanceOf[OutputFormat[_,_]]
                     .getRecordWriter(taskContext)

    recordWriters.getOrElseUpdate((tag, sinkId), mkRecordWriter)
  }
}


/** Object that allows for channels with different output format requirements
  * to be specified. */
object ChannelOutputFormat {
  /** format of output file names */
  val OutputChannelFileName = """ch(\d+)out(\d+)-.-\d+.*""".r

  /** @return true if the file path has the name of an output channel with the proper tag and index or if it is a _SUCCESS file */
  def isResultFile(tag: Int, sinkId: Int) =
    (f: Path) => f.getName match {
      case OutputChannelFileName(t, i) => t.toInt == tag && i.toInt == sinkId
      case _                    => false
    }

  private def propertyPrefix(tag: Int, sinkId: Int) = "scoobi.output." + tag + ":" + sinkId
  private def formatProperty(tag: Int, sinkId: Int) = propertyPrefix(tag, sinkId) + ".format"
  private def keyClassProperty(tag: Int, sinkId: Int) = propertyPrefix(tag, sinkId) + ".key"
  private def valueClassProperty(tag: Int, sinkId: Int) = propertyPrefix(tag, sinkId) + ".value"
  private def otherProperty(tag: Int, sinkId: Int) = propertyPrefix(tag, sinkId) + ":"

  /** Add a new output channel. */
  def addOutputChannel(job: Job, tag: Int, sink: Sink)(implicit sc: ScoobiConfiguration) = {
    val conf = job.getConfiguration
    conf.set(formatProperty    (tag, sink.id), sink.outputFormat.getName)
    conf.set(keyClassProperty  (tag, sink.id), sink.outputKeyClass.getName)
    conf.set(valueClassProperty(tag, sink.id), sink.outputValueClass.getName)

    val jobCopy = new Job(new Configuration(conf))
    sink.outputConfigure(jobCopy)
    Option(jobCopy.getConfiguration.get(CACHE_FILES)).foreach { files =>
      conf.set(otherProperty(tag, sink.id) + CACHE_FILES, files)
    }
    conf.updateWith(jobCopy.getConfiguration) { case (k, v) if k != CACHE_FILES =>
      (otherProperty(tag, sink.id) + k, v)
    }
  }
}

