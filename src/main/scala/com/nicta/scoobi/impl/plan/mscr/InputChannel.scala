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
import org.kiama.rewriting.Rewriter

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

  // seq of tags that this channel leads to
  def tags: Seq[Int]
  def setup(implicit configuration: Configuration)
  def map[K1, V1, A, TaggedKey, TaggedValue, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)(implicit configuration: Configuration)
  def cleanup[K1, V1, TaggedKey, TaggedValue, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)(implicit configuration: Configuration)
}

case class MapperInputChannel(sourceNode: CompNode) extends InputChannel with Rewriter {

  override def toString = "MapperInputChannel("+sourceNode+")"

  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.sourceNode.id == sourceNode.id
    case _                     => false
  }

  /** collect all the tags accessible from this source node */
  lazy val tags = keyTypes.tags

  lazy val keyTypes: KeyTypes = {
    var types = KeyTypes()
    rewrite(sometd(query { case g: GroupByKey[_,_] => types = types.add(g.id, g.mwfk.wf, g.gpk) }))(sourceNode)
    types
  }
  lazy val valueTypes: ValueTypes = {
    var types = ValueTypes()
    rewrite(sometd(query { case g: GroupByKey[_,_] => types = types.add(g.id, g.mwfv.wf) }))(sourceNode)
    types
  }

  def setup(implicit configuration: Configuration) {
    rewrite(sometd(query { case pd: ParallelDo[_,_,_] => pd.setup(configuration) }))(sourceNode)
  }

  def map[K1, V1, A, TaggedKey, TaggedValue, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)(implicit configuration: Configuration) {
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

  def cleanup[K1, V1, TaggedKey, TaggedValue, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)(implicit configuration: Configuration) {
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

case class IdInputChannel(input: CompNode, gbk: Option[GroupByKey[_,_]] = None) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def tags = Seq(gbk.map(_.id).getOrElse(input.id))
}

object InputChannel {
  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}
