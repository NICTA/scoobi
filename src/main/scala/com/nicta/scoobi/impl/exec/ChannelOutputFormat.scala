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
package exec

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.filecache.DistributedCache._
import scala.collection.mutable.{Map => MMap}

import io.DataSink
import Configurations._


/** A class that simplifies writing output to different paths and with different types
  * depending on the output channel and required outputs per channel. */
class ChannelOutputFormat(context: TaskInputOutputContext[_, _, _, _]) {

  private val taskContexts: MMap[(Int, Int), TaskAttemptContext] = MMap.empty
  private val recordWriters: MMap[(Int, Int), RecordWriter[_,_]] = MMap.empty


  /** Write a value out on multiple outputs of a given output channel.*/
  def write[K, V](channel: Int, output: Int, kv: (K, V)) = {
    val taskContext = getContext(channel, output)
    val recordWriter = getRecordWriter(taskContext, channel, output).asInstanceOf[RecordWriter[K, V]]
    recordWriter.write(kv._1, kv._2)
  }

  /** Close all opened output channels. */
  def close() = recordWriters.values foreach { _.close(context) }

  private def getContext(channel: Int, output: Int): TaskAttemptContext = {
    /* The following trick leverages the instantiation of a record writer via
     * the job thus supporting arbitrary output formats. */
    def mkTaskContext = {
      val conf = context.getConfiguration
      val job = new Job(new Configuration(conf))

      /* Set standard properties. */
      val format = conf.getClass(ChannelOutputFormat.formatProperty(channel, output), null)
                       .asInstanceOf[Class[_ <: OutputFormat[_,_]]]
      job.setOutputFormatClass(format)
      job.setOutputKeyClass(conf.getClass(ChannelOutputFormat.keyClassProperty(channel, output), null))
      job.setOutputValueClass(conf.getClass(ChannelOutputFormat.valueClassProperty(channel, output), null))
      job.getConfiguration.set("mapreduce.output.basename", "ch" + channel + "out" + output)

      val PropertyPrefix = (ChannelOutputFormat.otherProperty(channel, output) + """(.*)""").r
      conf.toMap collect { case (PropertyPrefix(k), v) => (k, v) } foreach {
        case (k, v) => job.getConfiguration.set(k, v)
      }

      new TaskAttemptContext(job.getConfiguration, context.getTaskAttemptID())
    }

    taskContexts.getOrElseUpdate((channel, output), mkTaskContext)
   }

  private def getRecordWriter(taskContext: TaskAttemptContext, channel: Int, output: Int): RecordWriter[_,_] = {

    /* Get the record writer from context output format. */
    def mkRecordWriter =
      ReflectionUtils.newInstance(taskContext.getOutputFormatClass, taskContext.getConfiguration)
                     .asInstanceOf[OutputFormat[_,_]]
                     .getRecordWriter(taskContext)

    recordWriters.getOrElseUpdate((channel, output), mkRecordWriter)
  }
}


/** Object that allows for channels with different output format requirements
  * to be specified. */
object ChannelOutputFormat {

  private def propertyPrefix(ch: Int, ix: Int) = "scoobi.output." + ch + ":" + ix
  private def formatProperty(ch: Int, ix: Int) = propertyPrefix(ch, ix) + ".format"
  private def keyClassProperty(ch: Int, ix: Int) = propertyPrefix(ch, ix) + ".key"
  private def valueClassProperty(ch: Int, ix: Int) = propertyPrefix(ch, ix) + ".value"
  private def otherProperty(ch: Int, ix: Int) = propertyPrefix(ch, ix) + ":"

  /** Add a new output channel. */
  def addOutputChannel(job: Job, channel: Int, output: Int, sink: DataSink[_,_,_]) = {
    val conf = job.getConfiguration
    conf.set(formatProperty(channel, output), sink.outputFormat.getName)
    conf.set(keyClassProperty(channel, output), sink.outputKeyClass.getName)
    conf.set(valueClassProperty(channel, output), sink.outputValueClass.getName)

    val jobCopy = new Job(new Configuration(conf))
    sink.outputConfigure(jobCopy)
    Option(jobCopy.getConfiguration.get(CACHE_FILES)).foreach { files =>
      conf.set(otherProperty(channel, output) + CACHE_FILES, files)
    }
    conf.updateWith(jobCopy.getConfiguration) { case (k, v) if k != CACHE_FILES =>
      (otherProperty(channel, output) + k, v)
    }
  }
}
