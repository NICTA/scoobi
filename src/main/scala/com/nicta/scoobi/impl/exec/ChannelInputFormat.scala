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
import java.io.DataOutput
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils
import scala.util.matching.Regex


/** Object that allows for channels with different input format requirements
  * to be specified. Makes use of ChannelInputFormat. */
object ChannelInputFormat {

  private val INPUT_FORMAT_PROPERTY = "scoobi.input.formats"

  /** Add a new input channel. */
  def addInputChannel
      (conf: JobConf,
       channel: Int,
       inputPath: Path,
       inputFormat: Class[_ <: FileInputFormat[_,_]]) = {

    val inputFormatMapping = List(channel.toString,
                                  inputPath.toString,
                                  inputFormat.getName).mkString(";")
    val inputFormats = conf.get(INPUT_FORMAT_PROPERTY)

    if (inputFormats == null)
      conf.set(INPUT_FORMAT_PROPERTY, inputFormatMapping)
    else
      conf.set(INPUT_FORMAT_PROPERTY, inputFormats + "," + inputFormatMapping)

    conf.setInputFormat(classOf[ChannelInputFormat[_,_]])
  }


  /** Get a map of all the input channels. */
  def getInputChannels(conf: JobConf): Map[Int, (Path, InputFormat[_,_])] = {
    val Entry = """(.*);(.*);(.*)""".r

    conf.get(INPUT_FORMAT_PROPERTY).split(",").toList map {
      case Entry(ch, path, infmt) => (ch.toInt,
                                        (new Path(path),
                                        ReflectionUtils.newInstance(Class.forName(infmt), conf).asInstanceOf[InputFormat[_,_]]))
    } toMap
  }
}


/** An input format that delgates to multiple input formats, one for each
  * input channel. */
class ChannelInputFormat[K, V] extends InputFormat[K, V] {

  def getSplits(conf: JobConf, numSplits: Int): Array[InputSplit] = {

    ChannelInputFormat.getInputChannels(conf) flatMap { case (channel, (path, format)) =>

      val confCopy = new JobConf(conf)
      FileInputFormat.addInputPath(confCopy, path)

      format.getSplits(confCopy, numSplits) map { (pathSplit: InputSplit) =>
        new TaggedInputSplit(conf, channel, pathSplit, format.getClass.asInstanceOf[Class[_ <: InputFormat[_,_]]])
      }
    } toArray
  }

  def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): RecordReader[K, V] = {

    /* Find the InputFormat and then the RecordReader from the TaggedInputSplit. */
    val taggedInputSplit: TaggedInputSplit = split.asInstanceOf[TaggedInputSplit]
    val inputFormat = taggedInputSplit.inputFormatClass.newInstance.asInstanceOf[InputFormat[K, V]]

    inputFormat.getRecordReader(taggedInputSplit.inputSplit, conf, reporter)
  }
}


/** A wrapper around an InputSplit that is tagged with an input channel id. Is
  * used with ChannelInputForamt. */
class TaggedInputSplit
    (var conf: Configuration,
     var channel: Int,
     var inputSplit: InputSplit,
     var inputFormatClass: Class[_ <: InputFormat[_,_]])
  extends InputSplit {

  def this() = this(null.asInstanceOf[Configuration], 0, null.asInstanceOf[InputSplit],
                    null.asInstanceOf[Class[_ <: InputFormat[_,_]]])

  def getLength: Long = inputSplit.getLength

  def getLocations: Array[String] = inputSplit.getLocations

  def readFields(in: DataInput): Unit = {
    channel = in.readInt
    val inputSplitClassName = Text.readString(in)
    inputSplit = ReflectionUtils.newInstance(Class.forName(inputSplitClassName), conf).asInstanceOf[InputSplit]
    inputSplit.readFields(in)
    val inputFormatClassName = Text.readString(in)
    inputFormatClass = Class.forName(inputFormatClassName).asInstanceOf[Class[_ <: InputFormat[_,_]]]
  }

  def write(out: DataOutput): Unit = {
    out.writeInt(channel)
    Text.writeString(out, inputSplit.getClass.getName)
    inputSplit.write(out)
    Text.writeString(out, inputFormatClass.getName)
  }
}
