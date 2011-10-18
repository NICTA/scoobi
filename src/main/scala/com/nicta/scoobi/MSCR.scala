/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


object MSCRGraph {

  /* 
   * Precondition: The entire Smart.DList[_] AST must have been converted using
   * the @convert@ method. See method @DList.persist@
   */
  def apply(ci: ConvertInfo): MSCRGraph = {
    val mscrs = ci.mscrs.map{_.convert(ci)}.toSet

    val outputStores = {
      def toOutputStore(ds: DataStore with DataSink): Option[OutputStore] = {
        if ( ds.isInstanceOf[OutputStore] ) {
          Some(ds.asInstanceOf[OutputStore])
        } else {
          None
        }
      }

      def outputsOf(oc: OutputChannel): Set[DataStore with DataSink] = oc match {
        case GbkOutputChannel(outputs,_,_,_) => outputs
        case BypassOutputChannel(outputs,_)  => outputs
      }

      mscrs.flatMap{mscr => mscr.outputChannels.flatMap{outputsOf(_).flatMap{toOutputStore(_).toList}}}
    }

    new MSCRGraph(outputStores, mscrs)
  }

}

class MSCRGraph(val outputStores: Set[_ <: OutputStore], val mscrs: Set[MSCR])


/** A Map-Shuffle-Combiner-Reducer. Defined by a set of input and output
  * channels. */
case class MSCR
    (val inputChannels: Set[InputChannel],
     val outputChannels: Set[OutputChannel]) {

  /** The nodes that are inputs to this MSCR. */
  val inputNodes: Set[AST.Node[_]] = inputChannels map {
    case BypassInputChannel(store, _) => store.node
    case MapperInputChannel(store, _) => store.node
  }

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_]] = outputChannels flatMap {
    case BypassOutputChannel(outputs, _)    => outputs.map(_.node)
    case GbkOutputChannel(outputs, _, _, _) => outputs.map(_.node)
  }

}


/** Utilities for dealing with MSCRs. */
object MSCR {

  /** Locate the MSCR that contains a particular input distributed-list */
  def mscrContainingInput(mscrs: Set[MSCR], node: AST.Node[_]): MSCR = {
    mscrs.find(mscr => mscr.inputNodes.contains(node)).orNull
  }

  /** Locate the MSCR that contains a particular output distributed-list */
  def mscrContainingOutput(mscrs: Set[MSCR], node: AST.Node[_]): MSCR = {
    mscrs.find(mscr => mscr.outputNodes.contains(node)).orNull
  }
}


/** ADT for MSCR input channels. */
sealed abstract class InputChannel

case class BypassInputChannel[I <: DataStore with DataSource]
    (val input: I,
     origin: AST.Node[_] with KVLike[_,_])
  extends InputChannel

case class MapperInputChannel[I <: DataStore with DataSource]
    (val input: I,
     val mappers: Set[AST.Node[_] with MapperLike[_,_,_]])
  extends InputChannel



/** ADT for MSCR output channels. */
sealed abstract class OutputChannel

case class BypassOutputChannel[O <: DataStore with DataSink]
    (val outputs: Set[O],
     val origin: AST.Node[_])
  extends OutputChannel

case class GbkOutputChannel[O <: DataStore with DataSink]
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
