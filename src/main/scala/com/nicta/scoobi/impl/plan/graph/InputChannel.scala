package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import exec.UniqueId

/** ADT for MSCR input channels. */
sealed trait InputChannel

case class MapperInputChannel(var parDos: Set[ParallelDo[_,_,_]]) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
  def add(pd: ParallelDo[_,_,_]) = {
    parDos = parDos + pd
    this
  }
}

case class IdInputChannel(input: CompNode) extends InputChannel
case class StraightInputChannel(input: CompNode) extends InputChannel
