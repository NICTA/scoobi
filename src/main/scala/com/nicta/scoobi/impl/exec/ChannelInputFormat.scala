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

import java.io.IOException
import java.io.DataInput
import java.io.DataInputStream;
import java.io.DataOutput
import java.io.DataOutputStream;

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.serializer.Deserializer
import org.apache.hadoop.io.serializer.SerializationFactory
import org.apache.hadoop.io.serializer.Serializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils
import scala.util.matching.Regex
import scala.collection.JavaConversions._


/** Object that allows for channels with different input format requirements
  * to be specified. Makes use of ChannelInputFormat. */
object ChannelInputFormat {

  private val INPUT_FORMAT_PROPERTY = "scoobi.input.formats"

  /** Add a new input channel. */
  def addInputChannel
      (job: Job,
       channel: Int,
       inputPath: Path,
       inputFormat: Class[_ <: FileInputFormat[_,_]]) = {

    val conf = job.getConfiguration

    val inputFormatMapping = List(channel.toString,
                                  inputPath.toString,
                                  inputFormat.getName).mkString(";")
    val inputFormats = conf.get(INPUT_FORMAT_PROPERTY)

    if (inputFormats == null)
      conf.set(INPUT_FORMAT_PROPERTY, inputFormatMapping)
    else
      conf.set(INPUT_FORMAT_PROPERTY, inputFormats + "," + inputFormatMapping)

    job.setInputFormatClass(classOf[ChannelInputFormat[_,_]].asInstanceOf[Class[_ <: InputFormat[_,_]]])
  }


  /** Get a map of all the input channels. */
  def getInputChannels(context: JobContext): Map[Int, (Path, InputFormat[_,_])] = {
    val Entry = """(.*);(.*);(.*)""".r

    context.getConfiguration.get(INPUT_FORMAT_PROPERTY).split(",").toList map {
      case Entry(ch, path, infmt) =>
        (ch.toInt,
          (new Path(path),
          ReflectionUtils.newInstance(Class.forName(infmt), context.getConfiguration).asInstanceOf[InputFormat[_,_]]))
    } toMap
  }
}


/** An input format that delgates to multiple input formats, one for each
  * input channel. */
class ChannelInputFormat[K, V] extends InputFormat[K, V] {

  def getSplits(context: JobContext): java.util.List[InputSplit] = {

    ChannelInputFormat.getInputChannels(context) flatMap { case (channel, (path, format)) =>

      val conf = context.getConfiguration
      val jobCopy = new Job(conf)
      FileInputFormat.addInputPath(jobCopy, path)

      format.getSplits(jobCopy) map { (pathSplit: InputSplit) =>
        new TaggedInputSplit(conf, channel, pathSplit, format.getClass.asInstanceOf[Class[_ <: InputFormat[_,_]]])
      }

    } toList
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[K, V] = {
    new ChannelRecordReader(split, context)
  }
}


/** A RecordReader that delegates the functionality to the underlying record reader
  * in TaggedInputSplit. */
class ChannelRecordReader[K, V](split: InputSplit, context: TaskAttemptContext) extends RecordReader[K, V] {

  private val originalRR: RecordReader[K, V] = {
    val taggedInputSplit: TaggedInputSplit = split.asInstanceOf[TaggedInputSplit]
    val inputFormat = taggedInputSplit.inputFormatClass.newInstance.asInstanceOf[InputFormat[K, V]]
    inputFormat.createRecordReader(taggedInputSplit.inputSplit, context)
  }

  override def close() = originalRR.close()

  override def getCurrentKey: K = originalRR.getCurrentKey

  override def getCurrentValue: V = originalRR.getCurrentValue

  override def getProgress: Float = originalRR.getProgress

  override def initialize(split: InputSplit, context: TaskAttemptContext) =
    originalRR.initialize(split.asInstanceOf[TaggedInputSplit].inputSplit, context)

  override def nextKeyValue: Boolean = originalRR.nextKeyValue
}


/** A wrapper around an InputSplit that is tagged with an input channel id. Is
  * used with ChannelInputForamt. */
class TaggedInputSplit
    (private var conf: Configuration,
     var channel: Int,
     var inputSplit: InputSplit,
     var inputFormatClass: Class[_ <: InputFormat[_,_]])
  extends InputSplit with Configurable with Writable {

  def this() = this(null.asInstanceOf[Configuration], 0, null.asInstanceOf[InputSplit],
                    null.asInstanceOf[Class[_ <: InputFormat[_,_]]])

  def getLength: Long = inputSplit.getLength

  def getLocations: Array[String] = inputSplit.getLocations

  def readFields(in: DataInput): Unit = {
    channel = in.readInt
    val inputSplitClassName = Text.readString(in)
    inputSplit = ReflectionUtils.newInstance(Class.forName(inputSplitClassName), conf).asInstanceOf[InputSplit]
    val inputFormatClassName = Text.readString(in)
    inputFormatClass = Class.forName(inputFormatClassName).asInstanceOf[Class[_ <: InputFormat[_,_]]]

    val factory: SerializationFactory = new SerializationFactory(conf)
    val deserializer: Deserializer[InputSplit] = factory.getDeserializer(inputSplit.getClass.asInstanceOf[Class[InputSplit]])
    deserializer.open(in.asInstanceOf[DataInputStream])
    inputSplit = deserializer.deserialize(inputSplit)
  }

  def write(out: DataOutput): Unit = {
    out.writeInt(channel)
    Text.writeString(out, inputSplit.getClass.getName)
    Text.writeString(out, inputFormatClass.getName)

    val factory: SerializationFactory = new SerializationFactory(conf)
    val serializer: Serializer[InputSplit] = factory.getSerializer(inputSplit.getClass.asInstanceOf[Class[InputSplit]])
    serializer.open(out.asInstanceOf[DataOutputStream])
    serializer.serialize(inputSplit)
  }

  def getConf: Configuration = conf

  def setConf(conf: Configuration) = this.conf = conf
}
