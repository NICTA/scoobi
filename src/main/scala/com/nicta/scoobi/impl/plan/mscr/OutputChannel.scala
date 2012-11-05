package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._
import util._
import mapreducer.BridgeStore

/** ADT for MSCR output channels. */
trait OutputChannel extends Channel {
  lazy val id: Int = UniqueId.get

  /** sinks for this output channel */
  def sinks: Seq[Sink]
  def contains(node: CompNode): Boolean
  def environment: Option[CompNode]
}

trait MscrOutputChannel extends OutputChannel {
  def sinks = if (nodeSinks.isEmpty) Seq(bridgeStore) else nodeSinks
  protected def nodeSinks: Seq[Sink]
  def bridgeStore: Bridge
  def output: CompNode
  def contains(node: CompNode) = output == node
  def environment: Option[CompNode]
}
case class GbkOutputChannel(groupByKey:   GroupByKey[_,_],
                            var flatten:  Option[Flatten[_]]        = None,
                            var combiner: Option[Combine[_,_]]      = None,
                            var reducer:  Option[ParallelDo[_,_,_]] = None) extends MscrOutputChannel {

  override def toString =
    Seq(Some(groupByKey),
        flatten .map(n => "flatten  = "+n.toString),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")

  override def equals(a: Any) = a match {
    case o: GbkOutputChannel => o.groupByKey.id == groupByKey.id
    case _                   => false
  }

  /** @return the output node of this channel */
  def output = reducer.map(r => r: CompNode).orElse(combiner).orElse(flatten).getOrElse(groupByKey)

  def nodeSinks = groupByKey.sinks ++ flatten.toSeq.map(_.sinks).flatten ++
                                      combiner.toSeq.map(_.sinks).flatten ++
                                      reducer.toSeq.map(_.sinks).flatten

  def bridgeStore =
     (reducer: Option[CompNode]).
      orElse(combiner  ).
      orElse(flatten   ).
      getOrElse(groupByKey).bridgeStore

  def environment: Option[CompNode] = reducer.map(_.env)

}

case class BypassOutputChannel(output: ParallelDo[_,_,_]) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }
  def nodeSinks = output.sinks
  def bridgeStore = output.bridgeStore
  def environment: Option[CompNode] = Some(output.env)
}

case class FlattenOutputChannel(output: Flatten[_]) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannel => o.output.id == output.id
    case _ => false
  }
  def nodeSinks = output.sinks
  def bridgeStore = output.bridgeStore
  def environment: Option[CompNode] = None
}

object Channels extends control.ImplicitParameters {
  /** @return a sequence of distinct mapper input channels */
  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.parDos.map(_.id).toSet, in)).toMap.values.toSeq

  /** @return a sequence of distinct group by key output channels */
  def distinct(out: Seq[GbkOutputChannel])(implicit p: ImplicitParam): Seq[GbkOutputChannel] =
    out.map(o => (o.groupByKey.id, o)).toMap.values.toSeq

  /** @return a sequence of distinct bypass output channels */
  def distinct(out: Seq[BypassOutputChannel])(implicit p1: ImplicitParam1, p2: ImplicitParam2): Seq[BypassOutputChannel] =
    out.map(o => (o.output.id, o)).toMap.values.toSeq
}
