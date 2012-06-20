package com.nicta.scoobi
package impl
package plan
package graph

import comp._

/** ADT for MSCR input channels. */
sealed trait InputChannel

case class MapperInputChannel(parDos: Seq[ParallelDo[_,_,_]]) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
}

case class IdInputChannel(input: DComp[_, _ <: Shape]) extends InputChannel

