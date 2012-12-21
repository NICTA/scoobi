package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable
import core._
import comp._
import util.UniqueId
import exec.MapReduceJob
import mapreducer.{TaggedReducer, TaggedMapper, TaggedIdentityMapper}
import core.WireFormat._
import comp.GroupByKey
import CompNodes._
import scalaz.Equal
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.conf.Configuration

trait Channel extends Attributable

case class InputChannels(channels: Seq[InputChannel]) {
  def channel(n: Int) = channels(n)
}
case class OutputChannels(channels: Seq[OutputChannel]) {
  def channel(n: Int) = channels(n)
  def setup(implicit configuration: Configuration) { channels.foreach(_.setup) }
  def cleanup = channels.foreach(_.cleanup)
}
/**
 * ADT for MSCR input channels
 */
trait InputChannel extends Channel {
  lazy val id: Int = UniqueId.get
  override def equals(a: Any) = a match {
    case i: InputChannel => i.id == id
    case other           => false
  }
  override def hashCode = id.hashCode

  def keyTypes: KeyTypes
  def valueTypes: ValueTypes

  def setup()
  def map[K1, V1, A, TaggedKey, TaggedValue, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)
  def cleanup[K1, V1, TaggedKey, TaggedValue, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)()
}

case class MapperInputChannel(sourceNode: CompNode) extends InputChannel {

  override def toString = "MapperInputChannel("+sourceNode+")"

  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.sourceNode.id == sourceNode.id
    case _                     => false
  }

  /** must contains source, one per channel, exec code and output tags */
  def makeTaggedMapper = TaggedMapper(source)
  def taggedOutputs = outputs.map(TaggedOutput())

  def setup {
  //  mappers.foreach(mapper.setup())
  }

  def map[K1, V1, A, TaggedKey, TaggedValue, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {
    val v: A = converter.fromKeyValue(context, key, value)
    mappers foreach { mapper =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.map(v, emitter.asInstanceOf[Emitter[Any]])
    }
  }
  def cleanup[K1, V1, TaggedKey, TaggedValue, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {
    mappers foreach { case (env, mapper: TaggedMapper) =>
      val emitter = new Emitter[(K2, V2)] {
        def emit(x: (K2, V2)) {
          mapper.tags.foreach { tag =>
            tk.set(tag, x._1)
            tv.set(tag, x._2)
            context.write(tk, tv)
          }
        }
      }
      mapper.cleanup(env, emitter.asInstanceOf[Emitter[Any]])
    }
  }
}
object MapperInputChannel {
  def create(sourceNode: CompNode): MapperInputChannel = new MapperInputChannel(sourceNode)
}

case class IdInputChannel(input: CompNode,
                          allSources: InputChannel => Seq[Source] = (in: InputChannel) => Seq(),
                          gbk: Option[GroupByKey[_,_]] = None,
                          tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def inputs = Seq(input)
  def incomings = attributes.incomings(input)
  def outputs = attributes.outputs(input)
  def outgoings = attributes.outgoings(input)

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = Seq(input)
}

case class StraightInputChannel(input: CompNode,
                                allSources: InputChannel => Seq[Source] = (in: InputChannel) => Seq(),
                                tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _                       => false
  }

  def inputs = attributes.inputs(input).toSeq
  def incomings = attributes.incomings(input).toSeq
  def outputs = attributes.outputs(input).toSeq
  def outgoings = attributes.outgoings(input).toSeq

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = Seq(input)

}

object InputChannel {
  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}
