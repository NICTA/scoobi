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
    case BypassInputChannel(DInput(node, _), _)           => node
    case BypassInputChannel(DIntermediate(node, _, _), _) => node
    case BypassInputChannel(DOutput(node, _), _)          => node

    case MapperInputChannel(DInput(node, _), _)           => node
    case MapperInputChannel(DIntermediate(node, _, _), _) => node
    case MapperInputChannel(DOutput(node, _), _)          => node
  }

  /** The nodes that are outputs to this MSCR. */
  val outputNodes: Set[AST.Node[_]] = outputChannels map {
    case BypassOutputChannel(DIntermediate(node, _, _), _)    => node
    case BypassOutputChannel(DOutput(node, _), _)             => node

    case GbkOutputChannel(DIntermediate(node, _, _), _, _, _) => node
    case GbkOutputChannel(DOutput(node, _), _, _, _)          => node
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

case class BypassInputChannel[I <: DConnector with InputLike, K, V]
    (val input: I,
     origin: AST.Node[(K, V)])
    (implicit val mK: Manifest[K], val wtK: HadoopWritable[K], val ordK: Ordering[K],
              val mV: Manifest[V], val wtV: HadoopWritable[V],
              val mKV: Manifest[(K, V)], val wtKV: HadoopWritable[(K, V)])
  extends InputChannel

case class MapperInputChannel[I <: DConnector with InputLike]
    (val input: I,
     val mappers: Set[AST.Node[_] with MapperLike[_,_,_]])
  extends InputChannel



/** ADT for MSCR output channels. */
sealed abstract class OutputChannel

case class BypassOutputChannel[O <: DConnector with OutputLike,
                               B : Manifest, HadoopWritable]
    (val output: O,
     val origin: AST.Node[B])
  extends OutputChannel

case class GbkOutputChannel
    (val output: OutputLike,
     val flatten: Option[AST.Flatten[_]],
     val groupByKey: AST.GroupByKey[_,_],
     val combinerReducer: Option[Either[AST.CombinerReducer[_,_,_], Either[AST.Combiner[_,_], AST.Reducer[_,_,_]]]])
  extends OutputChannel
