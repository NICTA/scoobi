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
  /** @return the nodes which are part of this channel */
  def nodes: Seq[CompNode]
  def contains(node: CompNode) = nodes.contains(node)
  def environment: Option[CompNode]
  def tag: Int
  def setTag(t: Int): OutputChannel
}

trait MscrOutputChannel extends OutputChannel {
  def sinks = if (nodeSinks.isEmpty) bridgeStore.toSeq else nodeSinks
  protected def nodeSinks: Seq[Sink]
  def bridgeStore: Option[Bridge]
  def output: CompNode
  def environment: Option[CompNode]
}
case class GbkOutputChannel(groupByKey:   GroupByKey[_,_],
                            flatten:  Option[Flatten[_]]        = None,
                            combiner: Option[Combine[_,_]]      = None,
                            reducer:  Option[ParallelDo[_,_,_]] = None,
                            tag: Int = 0) extends MscrOutputChannel {

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

  def nodes: Seq[CompNode] = Seq[CompNode](groupByKey) ++ flatten.toSeq ++ combiner.toSeq ++ reducer.toSeq
  def setTag(t: Int) = copy(tag = t)
  /** @return the output node of this channel */
  def output = reducer.map(r => r: CompNode).orElse(combiner).orElse(flatten).getOrElse(groupByKey)

  def nodeSinks = nodes.flatMap(_.sinks)

  lazy val bridgeStore =
     (reducer: Option[CompNode]).
      orElse(combiner  ).
      getOrElse(groupByKey).bridgeStore

  def environment: Option[CompNode] = reducer.map(_.env)

}

case class BypassOutputChannel(output: ParallelDo[_,_,_], tag: Int = 0) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }
  def setTag(t: Int) = copy(tag = t)
  def nodes = Seq(output)
  def nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
  def environment: Option[CompNode] = Some(output.env)
}

case class FlattenOutputChannel(output: Flatten[_], tag: Int = 0) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannel => o.output.id == output.id
    case _ => false
  }
  def nodes: Seq[CompNode] = Seq(output)
  def setTag(t: Int) = copy(tag = t)
  def nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
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
