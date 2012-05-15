/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.plan

import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.impl.exec.MapperLike
import com.nicta.scoobi.impl.exec.ReducerLike


object MSCRGraph {

  /* 
   * Precondition: The entire Smart.DList[_] AST must have been converted using
   * the @convert@ method. See method @DList.persist@
   */
  def apply(ci: Smart.ConvertInfo): MSCRGraph = {
    val mscrs = ci.mscrs.map{_.convert(ci)}.toSet
    val outputs = ci.outMap.map { case (dcomp, sinks) => OutputStore(ci.astMap(dcomp), sinks) } .toList
    new MSCRGraph(outputs, mscrs)
  }
}

case class OutputStore(node: AST.Node[_], sinks: Set[_ <: DataSink[_,_,_]])
case class MSCRGraph(outputs: List[OutputStore], mscrs: Set[MSCR])


/** A Map-Shuffle-Combiner-Reducer. Defined by a set of input and output
  * channels. */
case class MSCR(inputChannels: Set[InputChannel], outputChannels: Set[OutputChannel]) {

  /** The nodes that are inputs to this MSCR. */
  val inputNodes: Set[AST.Node[_]] = inputChannels.map(_.inputNode)

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_]] = outputChannels.map(_.outputNode)
}


/** Utilities for dealing with MSCRs. */
object MSCR {

  /** Locate the MSCR that contains a particular input distributed-list */
  def containingInput(mscrs: Set[MSCR], node: AST.Node[_]): MSCR = {
    mscrs.find(mscr => mscr.inputNodes.contains(node)).orNull
  }

  /** Locate the MSCR that contains a particular output distributed-list */
  def containingOutput(mscrs: Set[MSCR], node: AST.Node[_]): MSCR = {
    mscrs.find(mscr => mscr.outputNodes.contains(node)).orNull
  }
}


/** ADT for MSCR input channels. */
sealed abstract class InputChannel {
  def inputNode: AST.Node[_]
}

case class BypassInputChannel(source: DataSource[_,_,_], origin: AST.Node[_] with KVLike[_,_]) extends InputChannel {
  def inputNode: AST.Node[_] = origin
}

case class StraightInputChannel(source: DataSource[_,_,_], origin: AST.Node[_]) extends InputChannel {
  def inputNode: AST.Node[_] = origin
}

abstract case class MapperInputChannel(source: DataSource[_,_,_], mappers: Set[AST.Node[_] with MapperLike[_,_,_]]) extends InputChannel


/** ADT for MSCR output channels. */
sealed abstract class OutputChannel {
  def outputNode: AST.Node[_]
}

case class BypassOutputChannel
    (outputs: Set[_ <: DataSink[_,_,_]],
     origin: AST.Node[_] with MapperLike[_,_,_] with ReducerLike[_,_,_])
  extends OutputChannel {

  def outputNode: AST.Node[_] = origin
}

case class StraightOutputChannel
(outputs: Set[_ <: DataSink[_,_,_]],
 origin: AST.Node[_] with ReducerLike[_,_,_])
  extends OutputChannel {

  def outputNode: AST.Node[_] = origin
}

case class FlattenOutputChannel
    (outputs: Set[_ <: DataSink[_,_,_]],
     flatten: AST.Flatten[_])
  extends OutputChannel {

  def outputNode: AST.Node[_] = flatten
}

case class GbkOutputChannel
    (val outputs: Set[_ <: DataSink[_,_,_]],
     val flatten: Option[AST.Flatten[_]],
     val groupByKey: AST.GroupByKey[_,_],
     val crPipe: CRPipe)
  extends OutputChannel {

  def outputNode: AST.Node[_] = crPipe match {
    case Empty                 => groupByKey
    case JustCombiner(c)       => c
    case JustReducer(r)        => r
    case CombinerReducer(_, r) => r
  }
}


/** ADT for the possible combiner-reducer pipeline combinations. */
sealed abstract class CRPipe
object Empty extends CRPipe
case class JustCombiner(combiner: AST.Combiner[_,_]) extends CRPipe
case class JustReducer(reducer: AST.GbkReducer[_,_,_]) extends CRPipe
case class CombinerReducer(combiner: AST.Combiner[_,_], reducer: AST.Reducer[_,_,_]) extends CRPipe
