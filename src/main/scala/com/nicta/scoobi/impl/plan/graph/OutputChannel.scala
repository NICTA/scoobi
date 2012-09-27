package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import exec.{BridgeStore, UniqueId}
import io.DataSink

/** ADT for MSCR output channels. */
trait OutputChannel extends Channel {
  lazy val id: Int = UniqueId.get

  /** sinks for this output channel */
  def sinks: Seq[DataSink[_,_,_]]
}

case class GbkOutputChannel(groupByKey:   GroupByKey[_,_],
                            var flatten:  Option[Flatten[_]]        = None,
                            var combiner: Option[Combine[_,_]]      = None,
                            var reducer:  Option[ParallelDo[_,_,_]] = None,
                            sinks:        Seq[DataSink[_,_,_]]      = Seq()) extends OutputChannel {

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

  def addSinks(sinks: Map[CompNode, Seq[DataSink[_,_,_]]]) =
    copy(sinks = sinks.get(output).getOrElse(Seq(BridgeStore())))

  /** @return the output node of this channel */
  def output = reducer.orElse(combiner).orElse(flatten).getOrElse(groupByKey)
}

case class BypassOutputChannel(output: ParallelDo[_,_,_], sinks: Seq[DataSink[_,_,_]] = Seq()) extends OutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }

  def addSinks(sinks: Map[CompNode, Seq[DataSink[_,_,_]]]) =
    copy(sinks = sinks.get(output).getOrElse(Seq(BridgeStore())))

}

case class FlattenOutputChannel(output: Flatten[_], sinks: Seq[DataSink[_,_,_]] = Seq()) extends OutputChannel {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannel => o.output.id == output.id
    case _ => false
  }
  def addSinks(sinks: Map[CompNode, Seq[DataSink[_,_,_]]]) =
    copy(sinks = sinks.get(output).getOrElse(Seq(BridgeStore())))
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
