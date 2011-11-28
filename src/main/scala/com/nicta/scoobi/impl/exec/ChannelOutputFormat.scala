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

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.io.NullWritable
import scala.util.matching.Regex
import scala.collection.mutable.{Map => MMap}


/** Object that allows for channels with different output format requirements
  * to be specified. */
object ChannelOutputFormat {

  private val OUTPUT_FORMAT_PROPERTY = "scoobi.output.formats"

  /** Add a new output channel. */
  def addOutputChannel
      (job: Job,
       channel: Int,
       output: Int,
       outputFormat: Class[_ <: OutputFormat[_,_]],
       valueClass: Class[_]) = {

    val conf = job.getConfiguration

    val outputFormatMapping = List(channel.toString, valueClass.getName).mkString(";")
    val outputFormats = conf.get(OUTPUT_FORMAT_PROPERTY)

    if (outputFormats == null)
      conf.set(OUTPUT_FORMAT_PROPERTY, outputFormatMapping)
    else
      conf.set(OUTPUT_FORMAT_PROPERTY, outputFormats + "," + outputFormatMapping)

    MultipleOutputs.addNamedOutput(job, "ch" + channel + "out" + output, outputFormat, classOf[NullWritable], valueClass)
  }


  /** Get the value class for a particular output channel. */
  def getValueClass(job: JobContext, channel: Int): Class[_] = {
  
    val Entry = """(.*);(.*)""".r

    val valueClasses = job.getConfiguration.get(OUTPUT_FORMAT_PROPERTY).split(",").toList map {
      case Entry(ch, outfmt) => (ch.toInt -> outfmt)
    } toMap
  
    Class.forName(valueClasses(channel))
  }
}


/** A class that simplifies writing output to different paths and with different types
  * depending on the output channel and required outputs per channel. */
class ChannelOutputFormat[V3](context: TaskInputOutputContext[_, _, NullWritable, V3]) {

  private val mos = new MultipleOutputs(context)

  /** Write a value out on multiple outputs of a given output channel.*/
  def write(channel: Int, numOutputs: Int, value: V3) = {
    (0 to numOutputs - 1) map { "ch" + channel + "out" + _ } foreach {
      mos.write(_,  NullWritable.get, value)
    }
  }

  /** Close all opened output channels. */
  def close() = mos.close()
}
