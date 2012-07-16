package com.nicta.scoobi
package impl
package plan
package graph

import comp._

/** ADT for MSCR input channels. */
sealed trait InputChannel

case class MapperInputChannel(var parDos: Seq[ParallelDo[_,_,_]]) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
  def add(pd: ParallelDo[_,_,_]) = {
    parDos = parDos :+ pd
    this
  }
}

case class IdInputChannel(input: DComp[_, _ <: Shape]) extends InputChannel

