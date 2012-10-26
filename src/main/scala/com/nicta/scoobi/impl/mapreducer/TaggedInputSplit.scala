package com.nicta.scoobi
package impl
package mapreducer

import java.io.{DataOutputStream, DataOutput, DataInputStream, DataInput}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapreduce.{InputFormat, InputSplit}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.serializer.{Serializer, Deserializer, SerializationFactory}

/** A wrapper around an InputSplit that is tagged with an input channel id. Is
  * used with ChannelInputFormat. */
class TaggedInputSplit(private var conf: Configuration,
                       var channel: Int,
                       var inputSplit: InputSplit,
                       var inputFormatClass: Class[_ <: InputFormat[_,_]]) extends InputSplit with Configurable with Writable {

  def this() = this(null.asInstanceOf[Configuration], 0, null.asInstanceOf[InputSplit],
    null.asInstanceOf[Class[_ <: InputFormat[_,_]]])

  def getLength: Long = inputSplit.getLength

  def getLocations: Array[String] = inputSplit.getLocations

  def readFields(in: DataInput) {
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

  def write(out: DataOutput) {
    out.writeInt(channel)
    Text.writeString(out, inputSplit.getClass.getName)
    Text.writeString(out, inputFormatClass.getName)

    val factory: SerializationFactory = new SerializationFactory(conf)
    val serializer: Serializer[InputSplit] = factory.getSerializer(inputSplit.getClass.asInstanceOf[Class[InputSplit]])
    serializer.open(out.asInstanceOf[DataOutputStream])
    serializer.serialize(inputSplit)
  }

  def getConf: Configuration = conf
  def setConf(conf: Configuration) { this.conf = conf }
}

