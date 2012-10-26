package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._
import util._

/** ADT for MSCR output channels. */
trait OutputChannel extends Channel {
  lazy val id: Int = UniqueId.get

  /** sinks for this output channel */
  def sinks: Seq[Sink]
}

case class GbkOutputChannel(groupByKey:   GroupByKey[_,_],
                            var flatten:  Option[Flatten[_]]        = None,
                            var combiner: Option[Combine[_,_]]      = None,
                            var reducer:  Option[ParallelDo[_,_,_]] = None,
                            sinks:        Seq[Sink]      = Seq()) extends OutputChannel {

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
}

case class BypassOutputChannel(output: ParallelDo[_,_,_], sinks: Seq[Sink] = Seq()) extends OutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }
}

case class FlattenOutputChannel(output: Flatten[_], sinks: Seq[Sink] = Seq()) extends OutputChannel {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannel => o.output.id == output.id
    case _ => false
  }
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
