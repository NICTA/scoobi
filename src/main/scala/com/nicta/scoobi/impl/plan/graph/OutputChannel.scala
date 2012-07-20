package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import exec.UniqueId

/** ADT for MSCR output channels. */
sealed trait OutputChannel

case class GbkOutputChannel(groupByKey: GroupByKey[_,_],
                            var flatten:    Option[Flatten[_]]        = None,
                            var combiner:   Option[Combine[_,_]]      = None,
                            var reducer:    Option[ParallelDo[_,_,_]] = None) extends OutputChannel {

  override def toString =
    Seq(Some(groupByKey),
        flatten .map(n => "flatten  = "+n.toString),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")
}

case class BypassOutputChannel(input: ParallelDo[_,_,_]) extends OutputChannel
case class FlattenOutputChannel(input: Flatten[_]) extends OutputChannel

object Channels extends control.ImplicitParameters {
  /** @return a sequence of distinct mapper input channels */
  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.parDos.map(_.id).toSet, in)).toMap.values.toSeq

  /** @return a sequence of distinct group by key output channels */
  def distinct(out: Seq[GbkOutputChannel])(implicit p: ImplicitParam): Seq[GbkOutputChannel] =
    out.map(o => (o.groupByKey.id, o)).toMap.values.toSeq

  /** @return a sequence of distinct bypass output channels */
  def distinct(out: Seq[BypassOutputChannel])(implicit p1: ImplicitParam1, p2: ImplicitParam2): Seq[BypassOutputChannel] =
    out.map(o => (o.input.id, o)).toMap.values.toSeq
}