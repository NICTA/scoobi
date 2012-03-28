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

import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
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

    val outputStores = {
      def toOutputStore(ds: DataStore with DataSink[_,_,_]): Option[OutputStore[_,_,_]] = {
        if ( ds.isInstanceOf[OutputStore[_,_,_]] ) {
          Some(ds.asInstanceOf[OutputStore[_,_,_]])
        } else {
          None
        }
      }

      def outputsOf(oc: OutputChannel): Set[DataStore with DataSink[_,_,_]] = oc match {
        case GbkOutputChannel(outputs,_,_,_) => outputs
        case BypassOutputChannel(outputs,_)  => outputs
        case FlattenOutputChannel(outputs, _) => outputs
      }

      mscrs.flatMap{mscr => mscr.outputChannels.flatMap{outputsOf(_).flatMap{toOutputStore(_).toList}}}
    }

    new MSCRGraph(outputStores, mscrs)
  }
}

class MSCRGraph(val outputStores: Set[_ <: OutputStore[_,_,_]], val mscrs: Set[MSCR])


/** A Map-Shuffle-Combiner-Reducer. Defined by a set of input and output
  * channels. */
case class MSCR
    (val inputChannels: Set[InputChannel],
     val outputChannels: Set[OutputChannel]) {

  /** The nodes that are inputs to this MSCR. */
  val inputNodes: Set[AST.Node[_]] = inputChannels map {
    case BypassInputChannel(store, _) => store.node
    case MapperInputChannel(store, _) => store.node
    case StraightInputChannel(store, _) => store.node
  }

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_]] = outputChannels flatMap {
    case BypassOutputChannel(outputs, _)    => outputs.map(_.node)
    case GbkOutputChannel(outputs, _, _, _) => outputs.map(_.node)
    case FlattenOutputChannel(outputs, _)   => outputs.map(_.node)
  }

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
sealed abstract class InputChannel

case class BypassInputChannel[I <: DataStore with DataSource[_,_,_]]
    (val input: I,
     origin: AST.Node[_] with KVLike[_,_])
  extends InputChannel

case class MapperInputChannel[I <: DataStore with DataSource[_,_,_]]
    (val input: I,
     val mappers: Set[AST.Node[_] with MapperLike[_,_,_]])
  extends InputChannel

case class StraightInputChannel[I <: DataStore with DataSource[_,_,_]]
    (val input: I,
     origin: AST.Node[_])
  extends InputChannel


/** ADT for MSCR output channels. */
sealed abstract class OutputChannel

case class BypassOutputChannel[O <: DataStore with DataSink[_,_,_]]
    (val outputs: Set[O],
     val origin: AST.Node[_] with MapperLike[_,_,_] with ReducerLike[_,_,_])
  extends OutputChannel

case class FlattenOutputChannel[O <: DataStore with DataSink[_,_,_]]
    (val outputs: Set[O],
     val flatten: AST.Flatten[_])
  extends OutputChannel

case class GbkOutputChannel[O <: DataStore with DataSink[_,_,_]]
    (val outputs: Set[O],
     val flatten: Option[AST.Flatten[_]],
     val groupByKey: AST.GroupByKey[_,_],
     val crPipe: CRPipe)
  extends OutputChannel


/** ADT for the possible combiner-reducer pipeline combinations. */
sealed abstract class CRPipe
object Empty extends CRPipe
case class JustCombiner(combiner: AST.Combiner[_,_]) extends CRPipe
case class JustReducer(reducer: AST.GbkReducer[_,_,_]) extends CRPipe
case class CombinerReducer(combiner: AST.Combiner[_,_], reducer: AST.Reducer[_,_,_]) extends CRPipe
