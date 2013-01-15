package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._
import util.UniqueInt
import mapreducer.{VectorEmitterWriter, ChannelOutputFormat}
import core.WireFormat._
import comp.GroupByKey
import scalaz.Equal
import org.apache.hadoop.conf.Configuration
import rtt._
import control.Functions._
import Channel._

/**
 * An input channel groups mapping operations from a single DataSource, attached to a source node (a Load node, or a GroupByKey
 * node from a previous Mscr for example).
 *
 * There are however more data inputs for an InputChannel since the environments of ParallelDos are inputs as well
 *
 * An InputChannel emits (key, values) of different types classified by an Integer tag, either:
 *
 *  - the GroupByKey node id that will consume the key/values
 *  - the ParallelDo node id that will consume the key/values
 *
 * The main functionality of an InputChannel is to map an input key/value to another key/value to be grouped or reduced
 * using the functions of ParallelDos.
 *
 * There are 2 main types of InputChannels:
 *
 *  - GbkInputChannel: this input channel outputs key/values to GroupByKeys
 *  - FloatingInputChannel: this input channel simply does some mapping for "floating" paralleldos
 *
 *  They both share some implementation in the MapperInputChannel trait.
 *
 *  An input channel can have no mappers at all. In that case the values from the source node are directly emitted with
 *  no transformation.
 *
 * Two InputChannels are equal if they have the same source id.
 */
trait InputChannel {
  def id: Int

  override def equals(a: Any) = a match {
    case i: InputChannel => i.id == id
    case _               => false
  }
  override def hashCode = id.hashCode

  /** main source node providing data for this input channel */
  def sourceNode: CompNode
  /** data source for this input channel */
  def source: Source
  /** sourceNode + environments for the parallelDo nodes */
  def inputNodes: Seq[CompNode]

  /** set of tags, which are node ids consuming the values produced by this input channel */
  def tags: Seq[Int]
  /** types of the keys which are emitted by this InputChannel, by tag */
  def keyTypes: KeyTypes
  /** types of the values which are emitted by this InputChannel, by tag */
  def valueTypes: ValueTypes

  /** setup the parallelDos of this input channel */
  def setup(context: InputOutputContext)
  /** emit a new key/value using the parallelDos functions */
  def map(key: Any, value: Any, context: InputOutputContext)
  /** setup the parallelDos of this input channel */
  def cleanup(context: InputOutputContext)
}

/**
 * Common implementation of InputChannel for GbkInputChannel and FloatingInputChannel
 */
trait MapperInputChannel extends InputChannel {
  /** attributes used to determine if parallelDos are floating */
  private val nodes = new MscrsDefinition {}; import nodes._

  lazy val id: Int = sourceNode.id

  /** data source for this input channel */
  lazy val source = sourceNode match {
    case n: Load        => n.source
    case n: ProcessNode => n.bridgeStore
  }

  /** sourceNode + environments for the parallelDo nodes */
  lazy val inputNodes = sourceNode +: mappers.map(_.env)

  /**
   * last mappers in the "tree" of mappers using the input channel source node
   * A mapper not the "last" if its parent is a parallelDo that is included in the list of mappers
   */
  lazy val lastMappers: Seq[ParallelDo] =
    if (mappers.size <= 1) mappers
    else                   mappers.filterNot(m => (isParallelDo && mappers.contains)(m.parent[CompNode]))

  /** flattened tree of mappers using this source */
  lazy val mappers = mappersUses(sourceNode)

  protected var tk: TaggedKey = _
  protected var tv: TaggedValue = _

  /** store the current TaggedKey/TaggedValue which are going to be a container for all key/values to map, by tag */
  def setup(context: InputOutputContext) {
    tk = context.context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]
    tv = context.context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  protected def scoobiConfiguration(configuration: Configuration) = ScoobiConfigurationImpl(configuration)

  /** map a given key/value and emit it */
  def map(key: Any, value: Any, context: InputOutputContext) {
    implicit val configuration = context.configuration
    implicit val sc = scoobiConfiguration(configuration)

    /**
     * map the input key/value using the full tree of mappers.
     *
     * we start from the leaves of the tree and compute values up to the top:
     *
     *  - the top value is the key/value passed by the context and converted by the Source
     *  - the internal mapper nodes are using a VectorEmitterWriter to compute values and leave them in memory
     *  - the in memory values are finally mapped with the "lastMappers" and emitted to disk
     */
    def computeMappers(node: CompNode, emitter: EmitterWriter): Seq[Any] = node match {
      case n if n == sourceNode => Seq(source.fromKeyValueConverter.asValue(context, key, value))
      case mapper: ParallelDo   =>
        val childNode =
          if (mapper.ins.size == 1) mapper.ins.head
          else                      mapper.ins.filter(n => transitiveUses(sourceNode).contains(n) || (sourceNode == n)).head

        val mappedValues = computeMappers(childNode, emitter)

        if (mappers.size > 1 && isInsideMapper(mapper)) VectorEmitterWriter().map(mappedValues, mapper)
        else                                            mappedValues.map(v => mapper.map(v, emitter))
      case _                    => Seq()
    }

    lastMappers.foreach(mapper => computeMappers(mapper, createEmitter(outputTag(mapper), context)))

    if (lastMappers.isEmpty)
      tags.map(t => createEmitter(t, context).write(source.fromKeyValueConverter.asValue(context, key, value)))
  }

  /** @return the output tag for a given "last" mapper */
  protected def outputTag(mapper: ParallelDo): Int

  def cleanup(context: InputOutputContext) {}

  /**
   * create an emitter for a given output tag, which will either emit values with auto-generated keys for a FloatingInputChannel,
   * or key/values for a GbkInputChannel
   */
  protected def createEmitter(tag: Int, context: InputOutputContext): EmitterWriter
}

/**
 * This input channel is a tree of Mappers which are all connected to Gbk nodes
 */
class GbkInputChannel(val sourceNode: CompNode, groupByKeys: Seq[GroupByKey]) extends MapperInputChannel {
  override def toString = "GbkInputChannel("+sourceNode+")"

  /** collect all the tags accessible from this source node */
  lazy val tags = keyTypes.tags

  lazy val keyTypes   = groupByKeys.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, cur.wfk, cur.gpk) }
  lazy val valueTypes = groupByKeys.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wfv) }

  protected def createEmitter(tag: Int, context: InputOutputContext) = new EmitterWriter {
    def write(x: Any) {
      x match {
        case (x1, x2) =>
          tk.set(tag, x1)
          tv.set(tag, x2)
          context.write(tk, tv)
      }
    }
  }
  protected def outputTag(mapper: ParallelDo): Int = mapper.parent[CompNode].id
}

/**
 * This input channel is a tree of Mappers which are not connected to Gbk nodes
 */
class FloatingInputChannel(val sourceNode: CompNode) extends MapperInputChannel {
  override def toString = "FloatingInputChannel("+sourceNode+")"

  /** collect all the tags accessible from this source node */
  lazy val tags = valueTypes.tags

  lazy val keyTypes   = lastMappers.foldLeft(KeyTypes())   { (res, cur) => res.add(cur.id, wireFormat[Int], Grouping.all) }
  lazy val valueTypes = lastMappers.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wf) }

  protected def createEmitter(tag: Int, context: InputOutputContext) = new EmitterWriter {
    def write(x: Any) {
      tk.set(tag, rollingInt.get)
      tv.set(tag, x)
      context.write(tk, tv)
    }
  }
  protected def outputTag(mapper: ParallelDo): Int = mapper.id
}


object InputChannel {
  def create(sourceNode: CompNode): MapperInputChannel = {
    /** attributes used to determine if parallelDos are floating */
    val nodes = new MscrsDefinition {}; import nodes._

    lazy val groupByKeys: Seq[GroupByKey] = groupByKeysUses(sourceNode)
    lazy val groupByKeysUses: CompNode => Seq[GroupByKey] = attr { case node =>
      val (gbks, nonGbks) = uses(node).partition(isGroupByKey)
      gbks.collect(isAGroupByKey).toSeq ++ nonGbks.filterNot(isMaterialise).flatMap(groupByKeysUses)
    }
    if (groupByKeys.isEmpty) new FloatingInputChannel(sourceNode)
    else                     new GbkInputChannel(sourceNode, groupByKeys)
  }

  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}

object Channel {
  object rollingInt extends UniqueInt
}

case class InputChannels(channels: Seq[InputChannel]) {
  def channelsForSource(n: Int) = channels.filter(_.source.id == n)
}

case class OutputChannels(channels: Seq[OutputChannel]) {
  def channel(n: Int) = channels.find(c => c.tag == n)
  def setup(implicit configuration: Configuration) { channels.foreach(_.setup) }
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) { channels.foreach(_.cleanup(channelOutput)) }
}
