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
package plan
package mscr

import org.apache.hadoop.conf.Configuration
import core._
import comp._
import mapreducer._
import core.WireFormat._
import scalaz.Equal
import rtt._
import control.Functions._
import Channel._
import core.InputOutputContext
import org.apache.commons.logging.LogFactory
import monitor.Loggable
import Loggable._
import org.apache.hadoop.mapreduce.task.{MapContextImpl, TaskInputOutputContextImpl}
import org.apache.hadoop.mapreduce.MapContext

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
 *  They both share some implementation in the MscrInputChannel trait.
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
trait MscrInputChannel extends InputChannel {
  implicit lazy val logger = LogFactory.getLog("scoobi.InputChannel")

  private val nodes = new MscrsDefinition {}; import nodes._

  lazy val id: Int = sourceNode.id

  /** data source for this input channel */
  lazy val source = sourceNode match {
    case n: Load        => n.source
    case n: ProcessNode => n.bridgeStore.getOrElse(n.createBridgeStore)
  }

  /** sourceNode + environments for the parallelDo nodes */
  lazy val inputNodes = sourceNode +: mappers.map(_.env)

  /**
   * last mappers in the "tree" of mappers using the input channel source node
   * A mapper is not the "last" if its parent is a parallelDo that is included in the list of mappers
   */
  def lastMappers: Seq[ParallelDo]

  /** collect all the mappers which are connected to the source node and connect to one of the terminal nodes for this channel */
  lazy val mappers =
    terminalNodes.flatMap(terminal => pathsToNode(sourceNode)(terminal)).
      // drop the source node from the path
      map(path => path.filterNot(_ == sourceNode)).
      // retain only the paths which contain parallelDos or a terminal node
      filter(_.forall(isParallelDo || terminalNodes.contains)).
      flatten.collect(isAParallelDo).distinct


  /** nodes defining the output values of this channel, group by keys for a GbkInputChannel or parallelDo nodes for a FloatingInputChannel */
  def terminalNodes: Seq[CompNode]

  private val indent = "\n          "
  override def toString =
    getClass.getSimpleName+"("+sourceNode+")" +
    mappersToString("mappers", mappers) +
    (if (mappers.nonEmpty) mappersToString("last mappers", lastMappers) else "")

  def mappersToString(name: String, mps: Seq[ParallelDo]) =
    "\n"+indent+
    (if (mps.isEmpty) s"no $name"
     else             s"$name ${mappers.mkString(indent, indent, "")}")

  protected var tks: Map[Int, TaggedKey] = Map()
  protected var tvs: Map[Int, TaggedValue] = Map()
  protected var emitters: Map[Int, EmitterWriter] = Map()
  protected var environments: Map[ParallelDo, Any] = Map()
  protected var vectorEmitter: VectorEmitterWriter = VectorEmitterWriter(null)
  protected implicit var configuration: Configuration = _
  protected implicit var scoobiConfiguration: ScoobiConfiguration = _

  /** store the current TaggedKey/TaggedValue which are going to be a container for all key/values to map, by tag */
  def setup(context: InputOutputContext) {
    configuration = context.configuration
    scoobiConfiguration = scoobiConfiguration(configuration)
    tks = Map(tags.map(t => { val key = context.context.getMapOutputKeyClass.newInstance.asInstanceOf[TaggedKey]; key.setTag(t); (t, key) }):_*)
    tvs = Map(tags.map(t => { val value = context.context.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]; value.setTag(t); (t, value) }):_*)
    tks.map { case (t, k) => k.configuration = configuration }
    tvs.map { case (t, v) => v.configuration = configuration }

    emitters = Map(tags.map(t => (t, createEmitter(t, context))):_*)
    vectorEmitter = VectorEmitterWriter(context)
    environments = Map(mappers.map(mapper => (mapper, mapper.environment(scoobiConfiguration))):_*)

    mappers.foreach(m => m.setup(environments(m)))
  }

  protected def scoobiConfiguration(configuration: Configuration) = ScoobiConfigurationImpl(configuration)

  /** memoise the mappers tree to improve performance */
  private lazy val nextMappers: CompNode => Seq[ParallelDo] = attr {
    case node => uses(node).collect(isAParallelDo).toSeq.filter(mappers.contains)
  }
  /** memoise the final mappers tree to improve performance */
  private lazy val isFinal: CompNode => Boolean = attr {
    case node => lastMappers.contains(node)
  }
  /** map a given key/value and emit it */
  def map(key: Any, value: Any, context: InputOutputContext) {

    val sourceValue = source.fromKeyValueConverter.asValue(context, key, value)
    computeNext(sourceNode, Seq(sourceValue))

    def computeNext(node: CompNode, inputValues: Seq[Any]): Seq[Any] = {
      nextMappers(node).flatMap { m =>
        val mapperResult = computeMapper(m, inputValues)
        if (isFinal(m)) emitValues(m, mapperResult)
        computeNext(m, mapperResult)
      }
    }
    def emitValues(mapper: ParallelDo, resultValues: Seq[Any]) {
      outputTags(mapper).map(emitters).foreach { emitter =>
        resultValues.foreach(emitter.write)
      }
    }

    def computeMapper(mapper: ParallelDo, inputValues: Seq[Any]): Seq[Any] =
      vectorEmitter.map(environments(mapper), inputValues, mapper)

    if (lastMappers.isEmpty)
      tags.map(t => emitters(t).write(sourceValue))
  }

  /** @return the output tag for a given "last" mapper */
  protected def outputTags(mapper: ParallelDo): Seq[Int]

  def cleanup(context: InputOutputContext) {
    lastMappers.foreach { m =>
      outputTags(m).foreach(t => m.cleanup(environments(m), emitters(t)))
    }.debug("finished cleaning up the mapper")
  }

  /**
   * create an emitter for a given output tag, which will either emit values with auto-generated keys for a FloatingInputChannel,
   * or key/values for a GbkInputChannel
   */
  protected def createEmitter(tag: Int, context: InputOutputContext): EmitterWriter
}

/**
 * This input channel is a tree of Mappers which are all connected to Gbk nodes
 */
class GbkInputChannel(val sourceNode: CompNode, groupByKeys: Seq[GroupByKey]) extends MscrInputChannel {
  private val nodes = new MscrsDefinition {}; import nodes._

  /** collect all the tags accessible from this source node */
  lazy val tags = keyTypes.tags

  lazy val terminalNodes = groupByKeys
  lazy val keyTypes   = groupByKeys.foldLeft(KeyTypes()) { (res, cur) => res.add(cur.id, cur.wfk, cur.gpk) }
  lazy val valueTypes = groupByKeys.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wfv) }

  lazy val lastMappers: Seq[ParallelDo] =
    if (mappers.size <= 1) mappers
    else                   mappers.filter(m => uses(m).exists(terminalNodes.contains))

  protected def createEmitter(tag: Int, ioContext: InputOutputContext) = new EmitterWriter with InputOutputContextScoobiJobContext {
    val (key, value) = (tks(tag), tvs(tag))
    def write(x: Any) {
      x match {
        case (x1, x2) =>
          key.set(x1)
          value.set(x2)
          ioContext.write(key, value)
      }
    }
    def context = ioContext
  }
  protected def outputTags(mapper: ParallelDo): Seq[Int] = uses(mapper).collect(isAGroupByKey).map(_.id).toSeq
}

/**
 * This input channel is a tree of Mappers which are not connected to Gbk nodes
 */
class FloatingInputChannel(val sourceNode: CompNode, val terminalNodes: Seq[CompNode]) extends MscrInputChannel {
  private val nodes = new MscrsDefinition {}; import nodes._

  /** collect all the tags accessible from this source node */
  lazy val tags = valueTypes.tags

  lazy val keyTypes   = lastMappers.foldLeft(KeyTypes())   { (res, cur) => res.add(cur.id, wireFormat[Int], Grouping.all) }
  lazy val valueTypes = lastMappers.foldLeft(ValueTypes()) { (res, cur) => res.add(cur.id, cur.wf) }

  lazy val lastMappers: Seq[ParallelDo] =
    if (mappers.size <= 1) mappers
    else                   mappers.filter(terminalNodes.contains)

  protected def createEmitter(tag: Int, ioContext: InputOutputContext) = new EmitterWriter with InputOutputContextScoobiJobContext {
    val (key, value) = (tks(tag), tvs(tag))

    def write(x: Any) {
      key.set(rollingInt.get)
      value.set(x)
      ioContext.write(key, value)
    }
    def context = ioContext
  }
  protected def outputTags(mapper: ParallelDo): Seq[Int] = Seq(mapper.id)
}


object InputChannel {

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
  def setup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) { channels.foreach(_.setup(channelOutput)) }
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) { channels.foreach(_.cleanup(channelOutput)) }
}
