package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import exec.UniqueId

trait Channel
/** ADT for MSCR input channels. */
trait InputChannel extends Channel {
  lazy val id: Int = UniqueId.get
}

case class MapperInputChannel(var parDos: Set[ParallelDo[_,_,_]]) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
  def add(pd: ParallelDo[_,_,_]) = {
    parDos = parDos + pd
    this
  }
  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.parDos.map(_.id) == parDos.map(_.id)
    case _ => false
  }
}

case class IdInputChannel(input: CompNode) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _ => false
  }
}
case class StraightInputChannel(input: CompNode) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _ => false
  }
}
