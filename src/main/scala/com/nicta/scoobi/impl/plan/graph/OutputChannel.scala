package com.nicta.scoobi
package impl
package plan
package graph

import comp._

/** ADT for MSCR output channels. */
sealed trait OutputChannel

case class GbkOutputChannel(groupByKey: GroupByKey[_,_],
                            flatten:    Option[Flatten[_]]        = None,
                            combiner:   Option[Combine[_,_]]      = None,
                            reducer:    Option[ParallelDo[_,_,_]] = None) extends OutputChannel {
  override def toString =
    Seq(Some(groupByKey),
        flatten .map(n => "flatten  = "+n.toString),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")
}

