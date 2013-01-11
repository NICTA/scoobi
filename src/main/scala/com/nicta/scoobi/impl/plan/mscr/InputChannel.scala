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
import org.apache.hadoop.mapreduce.{TaskInputOutputContext, TaskAttemptContext, Mapper}
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
  def setup(context: InputOutputContext)
  def map(key: Any, value: Any, context: InputOutputContext)
  def cleanup(context: InputOutputContext)
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
    case n: Load     => n.source
    case n: ProcessNode => n.bridgeStore
  }

  lazy val inputNodes = Seq(sourceNode) ++ mappers.map(_.env)

  lazy val keyTypes: KeyTypes =
    if (groupByKeys.isEmpty) lastMappers.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, wireFormat[Int], Grouping.all) }
    else                     groupByKeys.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, cur.wfk, cur.gpk) }

  lazy val valueTypes: ValueTypes =
    if (groupByKeys.isEmpty) lastMappers.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wf) }
    else                     groupByKeys.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wfv) }

  lazy val groupByKeys: Seq[GroupByKey] = groupByKeysUses(sourceNode)
  lazy val groupByKeysUses: CompNode => Seq[GroupByKey] = attr { case node =>
    val (gbks, nonGbks) = nodes.uses(node).partition(isGroupByKey)
    gbks.collect(isAGroupByKey).toSeq ++ nonGbks.filterNot(isMaterialise).flatMap(groupByKeysUses)
  }

  lazy val lastMappers: Seq[ParallelDo] =
    if (mappers.size <= 1) mappers
    else                   mappers.filterNot(m => isParallelDo(m.parent[CompNode]) && mappers.contains(m.parent[CompNode]))

  lazy val mappers: Seq[ParallelDo] = mappersUses(sourceNode)
  lazy val mappersUses: CompNode => Seq[ParallelDo] = attr { case node =>
    val (pds, _) = nodes.uses(node).partition(isParallelDo)
    pds.collect(isAParallelDo).toSeq ++ pds.filter { pd =>
      (isParallelDo && !isFloating)(pd.parent[CompNode])
    }.flatMap(mappersUses)
  }

  private var tk: TaggedKey = _
  private var tv: TaggedValue = _

  def setup(context: InputOutputContext) {
    tk = context.context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  private def valueEmitter(context: InputOutputContext) = new EmitterWriter {
    def write(x: Any) {
      tags.foreach { tag =>
        tk.set(tag, rollingInt.get)
        tv.set(tag, x)
        context.write(tk, tv)
      }
    }
  }

  private def keyValueEmitter(context: InputOutputContext) = new EmitterWriter {
    def write(x: Any) {
      x match {
        case (x1, x2) =>
          tags.foreach { tag =>
            tk.set(tag, x1)
            tv.set(tag, x2)
            context.write(tk, tv)
          }
      }
    }
  }

  def map(key: Any, value: Any, context: InputOutputContext) {
    implicit val configuration = context.context.getConfiguration
    implicit val sc = ScoobiConfigurationImpl(configuration)
    val emitter = if (groupByKeys.isEmpty) valueEmitter(context) else keyValueEmitter(context)

    def computeMappers(node: CompNode): Seq[Any] = node match {
      case n if n == sourceNode => Seq(source.fromKeyValueConverter.asValue(context, key, value))
      case mapper: ParallelDo              =>
        val mappedValues = computeMappers {
          if (mapper.ins.size == 1) mapper.ins.head
          else                      mapper.ins.filter(n => nodes.transitiveUses(sourceNode).contains(n) || (sourceNode == n)).head
        }

        if (mappers.size > 1 && nodes.uses(mapper).forall(isParallelDo && !isFloating)) {
          val vb = new VectorBuilder[Any]
          mappedValues.foreach(v => mapper.map(v, new EmitterWriter { def write(v: Any) { vb += v } }))
          vb.result
        }
        else mappedValues.map(v => mapper.map(v, emitter))
      case _                                       => Seq()
    }
    lastMappers foreach computeMappers
    if (lastMappers.isEmpty)
      emitter.write(source.fromKeyValueConverter.asValue(context, key, value))
  }

  def cleanup(context: InputOutputContext) {}
}

object InputChannel {
  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}
