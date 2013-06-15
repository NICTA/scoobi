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

import scalaz.Scalaz._
import scalaz.syntax.std.indexedSeq._
import control.Functions._
import collection._
import comp._
import core._

trait MscrsDefinition extends Layering {

  /**
   * The nodes which are selected to define a layer are the nodes which delimitate outputs.
   *
   * What we want is to layer the computation graph so that output dependencies are respected:
   *
   * - if output1 depends on output2 then output2 must be computed first
   */
  def selectNode = !isValueNode && isLayerNode && !nodeHasBeenFilled

  def isLayerNode = (isMaterialised  || isGbkOutput || isEndNode || isCheckpoint)

  /** node at the end of the graph */
  def isEndNode: CompNode => Boolean = attr { n =>
    parent(n).map(isRoot).getOrElse(true)
  }

  def isMaterialised: CompNode => Boolean = attr {
    case n => uses(n).exists(isMaterialise || isOp)
  }

  def isCheckpoint: CompNode => Boolean = attr {
    case p: ProcessNode => p.hasCheckpoint
    case other          => false
  }

  def isGbkOutput: CompNode => Boolean = attr {
    case pd: ParallelDo                       => isReducer(pd)
    case cb @ Combine1(gbk: GroupByKey)       => parent(cb).map(!isReducingNode).getOrElse(true) && isUsedAtMostOnce(gbk)
    case gbk: GroupByKey                      => parent(gbk).map(!isReducingNode).getOrElse(true)
    case other                                => false
  }

  /**
   * a node is said to be reducing if it is in a "reducing chain of nodes" after a gbk
   *
   *  - parallelDo(combine(gbk)) // parallelDo and combine are reducing
   *  - combine(gbk)             // combine is reducing
   *  - parallelDo(gbk)          // parallelDo is reducing
   * @return
   */
  def isReducingNode: CompNode => Boolean = attr {
    case pd: ParallelDo          => isReducer(pd)
    case Combine1(_: GroupByKey) => true
    case other                   => false
  }

  lazy val layerGbks: Layer[T] => Seq[GroupByKey] = attr {
    case layer => layer.nodes.collect {
      case ParallelDo1(ins)          => (ins ++ ins.collect(isACombine).map(_.in)).collect(isAGroupByKey)
      case Combine1(gbk: GroupByKey) => Seq(gbk)
      case gbk: GroupByKey           => Seq(gbk)
    }.flatten
  }

  def floatingParallelDos: Layer[T] => Seq[ParallelDo] = attr {
    case layer => layer.nodes.filter(isParallelDo).collect { case n => {
        val childrenPds = (n +: (n -> descendentsUntil(selectNode || isGroupByKey))).collect(isAParallelDo).filterNot(isReducer || nodeHasBeenFilled)
        childrenPds.filterNot(_.ins.exists(childrenPds.contains))
      }
    }.flatten
  }

  /** all the mscrs for a given layer */
  lazy val mscrs: Layer[T] => Seq[Mscr] =
    attr { case layer => gbkMscrs(layer) ++ pdMscrs(layer) }
  
  /** Mscrs for parallel do nodes which are not part of a Gbk mscr */
  lazy val pdMscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    val inputChannels = floatingParallelDos(layer).flatMap { pd =>
      pd.ins.map(source => new FloatingInputChannel(source, layer.nodes.filterNot(isGroupByKey), this))
    }
    val outputChannels = inputChannels.flatMap(_.lastMappers.map(BypassOutputChannel(_)))
    makeMscrs(inputChannels, outputChannels)
  }

  /** Mscrs for mscrs built around gbk nodes */
  lazy val gbkMscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    makeMscrs(gbkInputChannels(layer), gbkOutputChannels(layer))
  }

  /**
   * make Mscrs by grouping input channels when their output go to the same output channel
   */
  private def makeMscrs(in: Seq[InputChannel], out: Seq[OutputChannel]): Seq[Mscr] = {
    if (out.isEmpty) Seq()
    else {
      // groups of input channels having at least one tag in common
      val channelsWithCommonTags = Seqs.groupWhen(in) { (i1: InputChannel, i2: InputChannel) => (i1.tags intersect i2.tags).nonEmpty }

      // create Mscr for each set of channels with common tags
      channelsWithCommonTags.map { taggedInputChannels =>
        val correspondingOutputTags = taggedInputChannels.flatMap(_.tags)
        val outputChannels = out.filter(o => correspondingOutputTags.contains(o.tag))
        Mscr.create(taggedInputChannels, (if (outputChannels.isEmpty) taggedInputChannels.map(_.sourceNode).collect { case pd: ParallelDo => BypassOutputChannel(pd) } else outputChannels))
      }
    }
  }

  /** create a gbk output channel for each gbk in the layer */
  lazy val gbkOutputChannels: Layer[T] => Seq[OutputChannel] = {
    attr { case layer =>
      layerGbks(layer).map(gbk => gbkOutputChannel(gbk))
    }
  }

  /**
\   * @return a gbk output channel based on the nodes which are following the gbk
   */
  protected def gbkOutputChannel(gbk: GroupByKey): GbkOutputChannel = {
    parents(gbk) match {
      case (c: Combine) +: (p: ParallelDo) +: rest if isReducer(p) => GbkOutputChannel(gbk, combiner = Some(c), reducer = Some(p))
      case (p: ParallelDo) +: rest                 if isReducer(p) => GbkOutputChannel(gbk, reducer = Some(p))
      case (c: Combine) +: rest                                    => GbkOutputChannel(gbk, combiner = Some(c))
      case _                                                       => GbkOutputChannel(gbk)
    }
  }

  lazy val gbkInputChannels: Layer[T] => Seq[MscrInputChannel] = attr { case layer =>
    layerGbkSourceNodes(layer).map { sourceNode =>
      val groupByKeyUses = transitiveUses(sourceNode).collect(isAGroupByKey).filter(layerGbks(layer).contains).toSeq
      new GbkInputChannel(sourceNode, groupByKeyUses, this)
    }
  }

  lazy val layerInputs: Layer[T] => Seq[CompNode] = attr { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {       case other         => Seq(other)
    }.distinct
  }

  lazy val inputNodes: Mscr => Seq[CompNode] =
    attr { case mscr => mscr.inputNodes }

  lazy val layerSourceNodes: Layer[T] => Seq[CompNode] = attr { case layer =>
    layer.nodes.flatMap(sourceNodes).distinct.filterNot(isValueNode)
  }
  lazy val layerGbkSourceNodes: Layer[T] => Seq[CompNode] = attr { case layer =>
    layerGbks(layer).flatMap(sourceNodes).distinct.filterNot(isValueNode)
  }
  lazy val sourceNodes: CompNode => Seq[CompNode] = attr { case node =>
    val (sources, nonSources) = (node -> children).partition(isSourceNode)
    (sources ++ nonSources.filter(isParallelDo).flatMap(sourceNodes)).distinct
  }
  lazy val isSourceNode: CompNode => Boolean = attr {
    case node if node.sinks.nonEmpty => nodeHasBeenFilled(node)
    case _                           => true
  }

  lazy val layerSinks: Layer[T] => Seq[Sink] =
    attr { case layer => mscrs(layer).flatMap(_.sinks).distinct }

  lazy val layerSources: Layer[T] => Seq[Source] =
    attr { case layer => mscrs(layer).flatMap(_.sources).distinct }

  lazy val layerBridgeSinks: Layer[T] => Seq[Bridge] =
    attr { case layer => layerSinks(layer).collect { case bs: Bridge => bs } }

  lazy val layerBridgeSources: Layer[T] => Seq[Bridge] =
    attr { case layer => layerSources(layer).collect { case bs: Bridge => bs } }

  /** collect all input nodes to the gbks of this layer */
  lazy val gbkInputs: Layer[T] => Seq[CompNode] = attr { case layer =>
    layer.nodes.flatMap(_ -> inputs).flatMap {
      case other          if layerGbks(layer).flatMap(_ -> inputs).contains(other) => Seq(other)
      case other                                                             => Seq()
    }.distinct
  }

  lazy val isReducer: ParallelDo => Boolean = attr {
    case pd @ ParallelDo1((cb @ Combine1((gbk: GroupByKey))) +: rest) => rest.isEmpty && isUsedAtMostOnce(pd) && isUsedAtMostOnce(cb) && isUsedAtMostOnce(gbk)
    case pd @ ParallelDo1((gbk: GroupByKey) +: rest)                  => rest.isEmpty && isUsedAtMostOnce(pd) && isUsedAtMostOnce(gbk)
    case _                                                            => false
  }
}
