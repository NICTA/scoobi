/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** A Map-Shuffle-Combiner-Reducer. Defined by a set of input and output
  * channels. */
case class MSCR
    (val inputChannels: Set[InputChannel],
     val outputChannels: Set[OutputChannel]) {

  /** The nodes that are inputs to this MSCR. */
  val inputNodes: Set[AST.Node[_]] = inputChannels map {
    case BypassInputChannel(InputStore(node, _), _)     => node
    case BypassInputChannel(BridgeStore(node, _, _), _) => node
    case MapperInputChannel(InputStore(node, _), _)     => node
    case MapperInputChannel(BridgeStore(node, _, _), _) => node
  }

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_]] = outputChannels map { oc =>

    def outputNode[O <: DataStore with DataSink](outputs: Set[O]) = outputs.head match {
      case BridgeStore(node, _, _)  => node
      case OutputStore(node, _)     => node
    }

    oc match {
      case BypassOutputChannel(outputs, _)    => outputNode(outputs)
      case GbkOutputChannel(outputs, _, _, _) => outputNode(outputs)
    }
  }
}


/** Utilities for dealing with MSCRs. */
object MSCR {

  /** Locate the MSCR that contains a particular distributed-list. */
  def containingMSCR(mscrs: Set[MSCR], node: AST.Node[_]): MSCR = {
    mscrs.find(mscr => (mscr.inputNodes ++ mscr.outputNodes).contains(node)).orNull
  }
}


/** ADT for MSCR input channels. */
sealed abstract class InputChannel

case class BypassInputChannel[I <: DataStore with DataSource, K, V]
    (val input: I,
     origin: AST.Node[(K, V)])
    (implicit val mK: Manifest[K], val wtK: HadoopWritable[K], val ordK: Ordering[K],
              val mV: Manifest[V], val wtV: HadoopWritable[V],
              val mKV: Manifest[(K, V)], val wtKV: HadoopWritable[(K, V)])
  extends InputChannel

case class MapperInputChannel[I <: DataStore with DataSource]
    (val input: I,
     val mappers: Set[AST.Node[_] with MapperLike[_,_,_]])
  extends InputChannel



/** ADT for MSCR output channels. */
sealed abstract class OutputChannel

case class BypassOutputChannel[O <: DataStore with DataSink,
                               B : Manifest, HadoopWritable]
    (val outputs: Set[O],
     val origin: AST.Node[B])
  extends OutputChannel

case class GbkOutputChannel[O <: DataStore with DataSink]
    (val outputs: Set[O],
     val flatten: Option[AST.Flatten[_]],
     val groupByKey: AST.GroupByKey[_,_],
     val crPipe: CRPipe)
  extends OutputChannel


/** ADT for the possible combiner-reducer pipeline combinations. */
sealed abstract class CRPipe
case class Empty extends CRPipe
case class JustCombiner(combiner: AST.Combiner[_,_]) extends CRPipe
case class JustReducer(reducer: AST.GbkReducer[_,_,_]) extends CRPipe
case class CombinerReducer(combiner: AST.Combiner[_,_], reducer: AST.Reducer[_,_,_]) extends CRPipe
