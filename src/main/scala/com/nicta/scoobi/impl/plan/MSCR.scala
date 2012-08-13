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

import io.DataSink
import io.DataSource
import exec._

object MSCRGraph {

  /* 
   * Precondition: The entire Smart.DList[_] AST must have been converted using
   * the @convert@ method. See method @DList.persist@
   */
  def apply(ci: Smart.ConvertInfo): MSCRGraph = {
    val mscrs = ci.mscrs.map{_.convert(ci)}.toSet
    val outputs = ci.outMap.map { case (dcomp, sinks) => OutputStore(ci.astMap(dcomp), sinks) } .toList
    new MSCRGraph(outputs, mscrs, ci.bridgeStoreMap.toMap, ci.envMap.toMap)
  }
}

case class OutputStore(node: AST.Node[_, _ <: Shape], sinks: Set[_ <: DataSink[_,_,_]])
case class MSCRGraph(
  outputs: List[OutputStore],
  mscrs: Set[MSCR],
  matTable: Map[AST.Node[_, _ <: Shape], BridgeStore[_]],
  environments: Map[AST.Node[_, _ <: Shape], Env[_]])


/** A Map-Shuffle-Combiner-Reducer. Defined by a set of input and output
  * channels. */
case class MSCR(inputChannels: Set[InputChannel], outputChannels: Set[OutputChannel]) {

  /** Input environment nodes (Exp nodes) that are inputs to this MSCR. */
  val inputEnvs: Set[AST.Node[_, _ <: Shape]] = {
    val icNodes = inputChannels flatMap {
      case mic@MapperInputChannel(_, _) => mic.inputEnvs
      case other                        => Set.empty[AST.Node[_, _ <: Shape]]
    }
    val ocNodes = outputChannels collect {
      case GbkOutputChannel(_, _, _, JustReducer(r, _))        => r.env
      case GbkOutputChannel(_, _, _, CombinerReducer(_, r, _)) => r.env
    }

    icNodes ++ ocNodes
  }

  /** The nodes that are inputs to this MSCR, both Arr and Exp flavours. */
  val inputNodes: Set[AST.Node[_, _ <: Shape]] = {
    val arrNodes = inputChannels.map(_.inputNode)
    arrNodes ++ inputEnvs
  }

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_, _ <: Shape]] = outputChannels.map(_.outputNode)

  /** All nodes captured by this MSCR. */
  val nodes: Set[AST.Node[_, _ <: Shape]] = inputChannels.flatMap(_.nodes) ++ outputChannels.flatMap(_.nodes)

  /** all the BridgeStores for this MSCR */
  lazy val bridgeStores = inputChannels.collect {
    case BypassInputChannel(bs @ BridgeStore(), _) => bs
    case MapperInputChannel(bs @ BridgeStore(), _) => bs
  }
}


/** Utilities for dealing with MSCRs. */
object MSCR {

  /** Locate the MSCR that contains a particular input distributed-list */
  def containingInput(mscrs: Set[MSCR], node: AST.Node[_, _ <: Shape]): MSCR = {
    mscrs.find(mscr => mscr.inputNodes.contains(node)).orNull
  }

  /** Locate the MSCR that contains a particular output distributed-list */
  def containingOutput(mscrs: Set[MSCR], node: AST.Node[_, _ <: Shape]): MSCR = {
    mscrs.find(mscr => mscr.outputNodes.contains(node)).orNull
  }
}


/** ADT for MSCR input channels. */
sealed abstract class InputChannel {
  def inputNode: AST.Node[_, _ <: Shape]
  def nodes: Set[AST.Node[_, _ <: Shape]]
}

case class BypassInputChannel(source: DataSource[_,_,_], origin: AST.Node[_, _ <: Shape] with KVLike[_,_]) extends InputChannel {
  def inputNode: AST.Node[_, _ <: Shape] = origin
  def nodes: Set[AST.Node[_, _ <: Shape]] = Set.empty
}

case class StraightInputChannel(source: DataSource[_,_,_], origin: AST.Node[_, _ <: Shape]) extends InputChannel {
  def inputNode: AST.Node[_, _ <: Shape] = origin
  def nodes: Set[AST.Node[_, _ <: Shape]] = Set.empty
}

abstract case class MapperInputChannel(
    source: DataSource[_,_,_],
    mappers: Set[(Env[_], AST.Node[_, _ <: Shape] with MapperLike[_,_,_,_])])
  extends InputChannel {

  def inputNode: AST.Node[_, _ <: Shape]
  def inputEnvs: Set[AST.Node[_, _ <: Shape]]
  def nodes: Set[AST.Node[_, _ <: Shape]]
}


/** ADT for MSCR output channels. */
sealed abstract class OutputChannel {
  def outputNode: AST.Node[_, _ <: Shape]
  def nodes: Set[AST.Node[_, _ <: Shape]]
}

case class BypassOutputChannel
    (outputs: Set[_ <: DataSink[_,_,_]],
     origin: AST.Node[_, _ <: Shape] with MapperLike[_,_,_,_] with ReducerLike[_,_,_,_])
  extends OutputChannel {

  def outputNode: AST.Node[_, _ <: Shape] = origin
  def nodes: Set[AST.Node[_, _ <: Shape]] = Set.empty
}

case class FlattenOutputChannel
    (outputs: Set[_ <: DataSink[_,_,_]],
     flatten: AST.Flatten[_])
  extends OutputChannel {

  def outputNode: AST.Node[_, _ <: Shape] = flatten
  def nodes: Set[AST.Node[_, _ <: Shape]] = Set(flatten)
}

case class GbkOutputChannel
    (val outputs: Set[_ <: DataSink[_,_,_]],
     val flatten: Option[AST.Flatten[_]],
     val groupByKey: AST.GroupByKey[_,_],
     val crPipe: CRPipe)
  extends OutputChannel {

  def outputNode: AST.Node[_, _ <: Shape] = crPipe match {
    case Empty                    => groupByKey
    case JustCombiner(c)          => c
    case JustReducer(r, _)        => r
    case CombinerReducer(_, r, _) => r
  }

  def nodes: Set[AST.Node[_, _ <: Shape]] = {
    val crNodes: Set[AST.Node[_, _ <: Shape]] = crPipe match {
      case Empty                    => Set.empty
      case JustCombiner(c)          => Set(c)
      case JustReducer(r, _)        => Set(r)
      case CombinerReducer(c, r, _) => Set(c, r)
    }
    val flattenNode: Set[AST.Node[_, _ <: Shape]] = flatten.toList.toSet
    val gbkNode: Set[AST.Node[_, _ <: Shape]] = Set(groupByKey)

    flattenNode ++ gbkNode ++ crNodes
  }
}


/** ADT for the possible combiner-reducer pipeline combinations. */
sealed abstract class CRPipe
object Empty extends CRPipe
case class JustCombiner(combiner: AST.Combiner[_,_]) extends CRPipe
case class JustReducer(reducer: AST.GbkReducer[_,_,_,_], env: Env[_]) extends CRPipe
case class CombinerReducer(combiner: AST.Combiner[_,_], reducer: AST.Reducer[_,_,_,_], env: Env[_]) extends CRPipe
