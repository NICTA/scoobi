package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable
import core._
import comp._
import util.UniqueId
import collection.IdSet

trait Channel extends Attributable

/** ADT for MSCR input channels. */
trait InputChannel extends Channel {
  protected val attributes = new CompNodes {}
  
  lazy val id: Int = UniqueId.get
  def inputs: Seq[CompNode]
  def incomings: Seq[CompNode] = inputs
}

case class MapperInputChannel(var parDos: Set[ParallelDo[_,_,_]]) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
  def add(pd: ParallelDo[_,_,_]) = {
    parDos = parDos + pd
    this
  }
  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.parDos.map(_.id) == parDos.map(_.id)
    case _                     => false
  }
  def inputs = parDos.flatMap(pd => attributes.inputs(pd)).toSeq
  override def incomings = parDos.flatMap(pd => attributes.incomings(pd)).toSeq
}
object MapperInputChannel {
  def apply(pd: ParallelDo[_,_,_]*): MapperInputChannel = new MapperInputChannel(IdSet(pd:_*))
}
case class IdInputChannel(input: CompNode) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def inputs = Seq(input)

  override def incomings = attributes.incomings(input).toSeq
}
case class StraightInputChannel(input: CompNode) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _                       => false
  }
  def inputs = attributes.inputs(input).toSeq
  override def incomings =  attributes.incomings(input).toSeq
}
