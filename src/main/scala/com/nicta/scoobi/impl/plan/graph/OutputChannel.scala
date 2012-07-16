package com.nicta.scoobi
package impl
package plan
package graph

import comp._

/** ADT for MSCR output channels. */
sealed trait OutputChannel

case class GbkOutputChannel(groupByKey: GroupByKey[_,_],
                            var flatten:    Option[Flatten[_]]        = None,
                            var combiner:   Option[Combine[_,_]]      = None,
                            var reducer:    Option[ParallelDo[_,_,_]] = None) extends OutputChannel {

  def set(n: Flatten[_])        = { flatten  = Some(n); this }
  def set(n: Combine[_,_])      = { combiner = Some(n); this }
  def set(n: ParallelDo[_,_,_]) = { reducer  = Some(n); this }

  override def toString =
    Seq(Some(groupByKey),
        flatten .map(n => "flatten  = "+n.toString),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")
}

