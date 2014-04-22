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

import core._
import collection._
import Seqs._
import comp._
import CompNodes._
import control.Functions._
import CollectFunctions._

/**
 * This trait processes the computation graph created out of DLists and creates map-reduce jobs from it.
 *
 * The algorithm consists in:
 *
 * - building layers of independent nodes in the graph
 * - finding the input nodes for the first layer
 * - reaching "output" nodes from the input nodes
 * - building output channels with those nodes
 * - building input channels connecting the output to the input nodes
 * - aggregating input and output channels as Mscr representing a full map reduce job
 * - iterating on any processing node that is not part of a Mscr
 */
trait MscrsDefinition extends Layering with Optimiser { outer =>
  /**
   * create layers of MapReduce jobs from the computation graph defined by the start node
   * where each layer contains independent map reduce jobs
   */
  def createMapReduceLayers(start: CompNode): Seq[Layer] =
    createLayers(Vector(start)).filterNot(_.isEmpty)

  /**
   * From start nodes in the graph and the list of already visited nodes, create new layers of MapReduce jobs
   */
  private def createLayers(startNodes: Seq[CompNode], visited: Seq[CompNode] = Vector()): Seq[Layer] =
    processLayers(startNodes.distinct, visited) match {
      case firstLayer +: rest => {
        val mscrLayer = createMscrs(inputNodes(firstLayer), visited)
        mscrLayer +: createLayers(startNodes, (visited ++ firstLayer ++ mscrLayer.nodes).distinct)
      }
      case _ => Vector()
    }

  /** @return non-empty layers of processing nodes */
  protected def processLayers(startNodes: Seq[CompNode], visited: Seq[CompNode]): Seq[Seq[ProcessNode]] =
    layersOf(startNodes.distinct)
      .map(_.filterNot(visited.contains))
      .map(_.collect(isAProcessNode))
      .filter(_.nonEmpty)
      .map(_.distinct)

  /**
   * create a layer of Mscrs from input nodes, making sure not to use already visited nodes
   */
  protected def createMscrs(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Layer =
    createMscrs(createInputOutputLayer(inputNodes, visited))

  /**
   * find the input and output channels on the layer, assemble them into Mscrs when they have common tags
   */
  protected def createMscrs(inputOutputLayer: Seq[CompNode]): Layer = {

    val outChannels = outputChannels(inputOutputLayer)
    val channelsWithCommonTags = groupInputChannelsByOutputTags(inputOutputLayer)

    // create Mscr for each set of channels with common tags
    Layer(channelsWithCommonTags.map { inputChannels =>
      val correspondingOutputTags = inputChannels.flatMap(_.tags)
      val out = outChannels.filter(o => correspondingOutputTags.contains(o.tag))

      Mscr.create(inputChannels, out)
    }.filterNot(_.isEmpty))
  }

  /**
   * get all the non-visited nodes going from an input nodes to an output
   */
  protected def createInputOutputLayer(inputNodes: Seq[CompNode], visited: Seq[CompNode]): Seq[CompNode] = {
    val layerNodes       = transitiveUsesUntil(inputNodes, isAnOutputNode)
    val outputs          = layerNodes.filter(isAnOutputNode).filterNot(visited.contains)
    val outputLayers     = layersOf(outputs, isAnOutputNode)
    val firstOutputLayer = outputLayers.dropWhile(l => !l.exists(outputs.contains)).headOption.map(_.filter(isAnOutputNode)).getOrElse(Vector()).distinct

    // some input process nodes might have been missed by in the inputNodes collection,
    // get them in by going back from the output nodes to the leaves
    // see #298
    val additional = outer.inputNodes(layerNodes.collect(isAProcessNode)).collect(isAProcessNode)
    // remove visited nodes or nodes which depend on a node in the first layer
    (additional ++ layerNodes).distinct.filterNot(n => firstOutputLayer.exists(out => transitiveUses(out).contains(n)))
      .filterNot(visited.contains)
      .distinct
  }

  private def transitiveUsesUntil(inputs: Seq[CompNode], until: CompNode => Boolean): Seq[CompNode] = {
    if (inputs.isEmpty) Vector()
    else {
      val (stop, continue) = inputs.flatMap(uses).toSeq.partition(until)
      stop ++ continue ++ transitiveUsesUntil(continue, until)
    }
  }

  /**
   * @return groups of input channels having the same output tags
   */
  protected def groupInputChannelsByOutputTags(layer: Seq[CompNode]): Seq[Seq[InputChannel]] = {
    Seqs.transitiveClosure(inputChannels(layer)) { (i1: InputChannel, i2: InputChannel) =>
      (i1.tags intersect i2.tags).nonEmpty
    }.map(_.list)
  }

  protected def inputChannels(layer: Seq[CompNode]): Seq[InputChannel] = gbkInputChannels(layer) ++ floatingInputChannels(layer)

  /**
   * @return Process or Load nodes which are children of the nodes parameters but not included in the group
   *         these "input nodes" don't include Return nodes or Op nodes because those inputs are retrieved via environments
   */
  protected def inputNodes(nodes: Seq[ProcessNode]): Seq[CompNode] =
    nodes.collect { case node =>
      children(node).filterNot(isValueNode || nodes.contains)
    }.flatten.distinct

  protected def gbkInputChannels(layer: Seq[CompNode]): Seq[GbkInputChannel] = {
    val gbks = layer.filter(isGroupByKey)
    val in = inputNodes(layer.collect(isAProcessNode))
    in.flatMap { inputNode =>
      val groupByKeyUses = transitiveUses(inputNode).collect(isAGroupByKey).filter(gbks.contains).toSeq
      if (groupByKeyUses.isEmpty) Vector()
      else                        Vector(new GbkInputChannel(inputNode, groupByKeyUses, this))
    }
  }

  protected def floatingInputChannels(layer: Seq[CompNode]): Seq[FloatingInputChannel] = {
    val gbkChannels = gbkInputChannels(layer)
    val inputs = inputNodes(layer.collect(isAProcessNode))
    val gbkMappers = gbkChannels.flatMap(_.mappers)

    inputs.map { inputNode =>
      val mappers = transitiveUses(inputNode)
        .collect(isAParallelDo)
        .filter(layer.contains)
        .filterNot(gbkMappers.contains)
        .toVector

      // the "terminal" nodes for the input channel are all the ParallelDos on the last layer
      // and all the parallelDos going to a Root node or Materialise parent
      val layers = layersOf(mappers)
      val lastLayerParallelDos = layers.lastOption.getOrElse(Vector()).collect(isAParallelDo)
      val outputParallelDos = layers.dropRight(1).map(_.filter(p => isParallelDo(p) && parent(p).exists(isRoot || isMaterialise))).flatten
      new FloatingInputChannel(inputNode, (lastLayerParallelDos ++ outputParallelDos).distinct, this)
    }.filterNot(_.isEmpty)
  }

  protected def gbkOutputChannels(layer: Seq[CompNode]): Seq[OutputChannel] = {
    val gbks = layer.collect(isAGroupByKey)
    gbks.map(gbk => gbkOutputChannel(gbk))
  }

  /**
   * @return a gbk output channel based on the nodes which are following the gbk
   */
  protected def gbkOutputChannel(gbk: GroupByKey): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p), nodes = this)
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p), nodes = this)
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c), nodes = this)
      case _                                                       => GbkOutputChannel(gbk, nodes = this)
    }
  }

  /** @return all output channels for a given layer */
  protected def outputChannels(layer: Seq[CompNode]): Seq[OutputChannel] =
    gbkOutputChannels(layer) ++ bypassOutputChannels(layer)

  /** @return the bypass output channels for a given layer */
  protected def bypassOutputChannels(layer: Seq[CompNode]): Seq[OutputChannel] = {
    val bypassMappers = inputChannels(layer).flatMap(_.bypassOutputNodes)
    bypassMappers.distinct.map(m => BypassOutputChannel(m, nodes = this))
  }

  /** @return true if a node is an input node for a given layer */
  protected def isAnInputNode(nodes: Seq[CompNode]): CompNode => Boolean = (node: CompNode) =>
    !isValueNode(node) &&
      (children(node).isEmpty || children(node).forall(!nodes.contains(_)))

  /**
   * @return true if a node is a candidate for outputing values
   */
  protected def isAnOutputNode: CompNode => Boolean = (isMaterialised  || isGroupByKey || isEndNode || isCheckpoint) && !isReturn

  /** node at the end of the graph */
  protected def isEndNode: CompNode => Boolean = attr { n =>
    parent(n).isEmpty
  }

  protected def isMaterialised: CompNode => Boolean = attr {
    case n => uses(n).exists(isMaterialise || isOp)
  }

  protected def isCheckpoint: CompNode => Boolean = attr {
    case p: ProcessNode => p.hasCheckpoint
    case other          => false
  }

  protected def isGbkOutput: CompNode => Boolean = attr {
    case pd: ParallelDo                       => isReducer(pd)
    case cb @ Combine1(gbk: GroupByKey)       => parent(cb).map(!isReducingNode).getOrElse(true) && isUsedAtMostOnce(gbk)
    case gbk: GroupByKey                      => parent(gbk).map(!isReducingNode).getOrElse(true)
    case other                                => false
  }

  protected lazy val isReducer: ParallelDo => Boolean = attr {
    case pd @ ParallelDo1((cb @ Combine1((gbk: GroupByKey))) +: rest) => rest.isEmpty && isReturn(pd.env) && isUsedAtMostOnce(pd) && isUsedAtMostOnce(cb) && isUsedAtMostOnce(gbk)
    case pd @ ParallelDo1((gbk: GroupByKey) +: rest)                  => rest.isEmpty && isReturn(pd.env) && isUsedAtMostOnce(pd) && isUsedAtMostOnce(gbk)
    case _                                                            => false
  }

  protected lazy val isAReducer: CompNode => Boolean = attr {
    case node: ParallelDo if isReducer(node) => true
    case _                                   => false
  }

  /**
   * a node is said to be reducing if it is in a "reducing chain of nodes" after a gbk
   *
   *  - parallelDo(combine(gbk)) // parallelDo and combine are reducing
   *  - combine(gbk)             // combine is reducing
   *  - parallelDo(gbk)          // parallelDo is reducing
   * @return
   */
  protected def isReducingNode: CompNode => Boolean = attr {
    case pd: ParallelDo          => isReducer(pd)
    case Combine1(_: GroupByKey) => true
    case other                   => false
  }
}