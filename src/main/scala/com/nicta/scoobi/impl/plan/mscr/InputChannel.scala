package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.{Attribution, Attributable}
import core._
import comp._
import util.{UniqueInt, UniqueId}
import exec.{InMemoryMode, MapReduceJob}
import mapreducer.{ChannelOutputFormat}
import core.WireFormat._
import comp.GroupByKey
import scalaz.Equal
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.conf.Configuration
import org.kiama.rewriting.Rewriter
import scala.collection.immutable.VectorBuilder
import rtt.{TaggedValue, TaggedKey}
import control.Functions._
import CollectFunctions._

trait Channel extends Attributable

case class InputChannels(channels: Seq[InputChannel]) {
  def channelsForSource(n: Int) = channels.filter(_.source.id == n)
}
case class OutputChannels(channels: Seq[OutputChannel]) {
  def channel(n: Int) = channels.find(c => c.tag == n)
  def setup(implicit configuration: Configuration) { channels.foreach(_.setup) }
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) { channels.foreach(_.cleanup(channelOutput)) }
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
  def source: Source
  def sourceNode: CompNode
  def inputNodes: Seq[CompNode]
  // seq of tags that this channel leads to
  def tags: Seq[Int]
  def setup[K1, V1](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)
  def map[K1, V1, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)
  def cleanup[K1, V1, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context)
}

case class MapperInputChannel(sourceNode: CompNode) extends InputChannel {
  private object rollingInt extends UniqueInt
  private val nodes = new MscrsDefinition {}
  import nodes._

  override def toString = "MapperInputChannel("+sourceNode+")"

  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.sourceNode.id == sourceNode.id
    case _                     => false
  }
  override def hashCode = sourceNode.id.hashCode

  /** collect all the tags accessible from this source node */
  lazy val tags = if (groupByKeys.isEmpty) valueTypes.tags else keyTypes.tags

  lazy val source = sourceNode match {
    case n: Load[_]           => n.source
    case n: Op[_,_,_]         => n.bridgeStore.get
    case n: GroupByKey[_,_]   => n.bridgeStore.get
    case n: Combine[_,_]      => n.bridgeStore.get
    case n: ParallelDo[_,_,_] => n.bridgeStore.get
  }

  lazy val inputNodes = Seq(sourceNode) ++ mappers.map(_.env)

  lazy val keyTypes: KeyTypes =
    if (groupByKeys.isEmpty) lastMappers.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, wireFormat[Int], Grouping.all) }
    else                     groupByKeys.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, cur.wfk.wf, cur.gpk) }

  lazy val valueTypes: ValueTypes =
    if (groupByKeys.isEmpty) lastMappers.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wf) }
    else                     groupByKeys.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wfv.wf) }

  lazy val groupByKeys: Seq[GroupByKey[_,_]] = groupByKeysUses(sourceNode)
  lazy val groupByKeysUses: CompNode => Seq[GroupByKey[_,_]] = attr { case node =>
    val (gbks, nonGbks) = nodes.uses(node).partition(isGroupByKey)
    gbks.collect(isAGroupByKey).toSeq ++ nonGbks.filterNot(isMaterialize).flatMap(groupByKeysUses)
  }

  lazy val lastMappers: Seq[ParallelDo[_,_,_]] =
    if (mappers.size <= 1) mappers
    else                   mappers.filterNot(m => isParallelDo(m.parent[CompNode]) && mappers.contains(m.parent[CompNode]))

  lazy val mappers: Seq[ParallelDo[_,_,_]] = mappersUses(sourceNode)
  lazy val mappersUses: CompNode => Seq[ParallelDo[_,_,_]] = attr { case node =>
    val (pds, _) = nodes.uses(node).partition(isParallelDo)
    pds.collect(isAParallelDo).toSeq ++ pds.filter { pd =>
      (isParallelDo && !isFloating)(pd.parent[CompNode])
    }.flatMap(mappersUses)
  }

  private var tk: TaggedKey = _
  private var tv: TaggedValue = _

  def setup[K1, V1](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {
    tk = context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  private def valueEmitter[K1, V1](context: Mapper[K1,V1,TaggedKey,TaggedValue]#Context) = new Emitter[Any] {
    def emit(x: Any) {
      tags.foreach { tag =>
        tk.set(tag, rollingInt.get)
        tv.set(tag, x)
        context.write(tk, tv)
      }
    }
  }

  private def keyValueEmitter[K1, V1](context: Mapper[K1,V1,TaggedKey,TaggedValue]#Context) = new Emitter[(Any, Any)] {
    def emit(x: (Any, Any)) {
      tags.foreach { tag =>
        tk.set(tag, x._1)
        tv.set(tag, x._2)
        context.write(tk, tv)
      }
    }
  }

  def map[K1, V1, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {
    implicit val configuration = context.getConfiguration
    implicit val sc = ScoobiConfigurationImpl(configuration)
    val emitter = if (groupByKeys.isEmpty) valueEmitter(context) else keyValueEmitter(context)

    def computeMappers(node: CompNode): Seq[Any] = node match {
      case n if n == sourceNode => Seq(source.inputConverter.asInstanceOf[InputConverter[K1, V1, Any]].fromKeyValue(context, key, value))
      case mapper: ParallelDo[_,_,_]              =>
        val mappedValues = computeMappers {
          if (mapper.ins.size == 1) mapper.ins.head
          else                      mapper.ins.filter(n => nodes.transitiveUses(sourceNode).contains(n) || (sourceNode == n)).head
        }

        if (mappers.size > 1 && nodes.uses(mapper).forall(isParallelDo && !isFloating)) {
          val vb = new VectorBuilder[Any]
          mappedValues.foreach(v => mapper.unsafeMap(v, new Emitter[Any] { def emit(v: Any) { vb += v } }))
          vb.result
        }
        else mappedValues.map(v => mapper.unsafeMap(v, emitter))
      case _                                       => Seq()
    }
    lastMappers foreach computeMappers
    if (lastMappers.isEmpty)
      emitter.emit(source.inputConverter.asInstanceOf[InputConverter[K1, V1, (Any, Any)]].fromKeyValue(context, key, value))
  }

  def cleanup[K1, V1, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {}
}

case class IdInputChannel(input: CompNode, gbk: Option[GroupByKey[_,_]] = None) extends InputChannel {
  val sourceNode = input
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }
  override def hashCode = input.id.hashCode
  lazy val source = input match {
    case n: Load[_]           => n.source
    case n: GroupByKey[_,_]   => n.bridgeStore.get
    case n: Combine[_,_]      => n.bridgeStore.get
    case n: ParallelDo[_,_,_] => n.bridgeStore.get
  }
  lazy val inputNodes = input match {
    case n: ParallelDo[_,_,_] => Seq(n, n.env)
    case n                    => Seq(n)
  }

  lazy val keyTypes: KeyTypes = KeyTypes()
  lazy val valueTypes: ValueTypes = ValueTypes().add(input.id, input.wf)

  def tags = Seq(gbk.map(_.id).getOrElse(input.id))

  def setup[K1, V1](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {}
  def map[K1, V1, K2, V2](key: K1, value: V1, context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {}
  def cleanup[K1, V1, K2, V2](context: Mapper[K1, V1, TaggedKey, TaggedValue]#Context) {}
}

object InputChannel {
  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}
