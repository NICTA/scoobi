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
  def outputs: Seq[CompNode]

  def incomings: Seq[CompNode]
  def outgoings: Seq[CompNode]

  def setTags(ts: Set[Int]): InputChannel
  def tags: Set[Int]
  def contains(node: CompNode): Boolean
}

case class MapperInputChannel(var parDos: Set[ParallelDo[_,_,_]], tags: Set[Int] = Set(0)) extends InputChannel {
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
  def incomings = parDos.flatMap(pd => attributes.incomings(pd)).toSeq

  def outputs = parDos.flatMap(pd => attributes.outputs(pd)).toSeq
  def outgoings = parDos.flatMap(pd => attributes.outgoings(pd)).toSeq

  def setTags(ts: Set[Int]): InputChannel = copy(tags = ts)
  def contains(node: CompNode): Boolean = parDos.toSeq.contains(node)
}
object MapperInputChannel {
  def apply(pd: ParallelDo[_,_,_]*): MapperInputChannel = new MapperInputChannel(IdSet(pd:_*))
}

case class IdInputChannel(input: CompNode, tags: Set[Int] = Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def inputs = Seq(input)
  def incomings = attributes.incomings(input).toSeq
  def outputs = attributes.outputs(input).toSeq
  def outgoings = attributes.outgoings(input).toSeq

  def setTags(ts: Set[Int]): InputChannel = copy(tags = ts)
  def contains(node: CompNode): Boolean = input.id == node.id
}

case class StraightInputChannel(input: CompNode, tags: Set[Int] = Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _                       => false
  }

  def inputs = attributes.inputs(input).toSeq
  def incomings = attributes.incomings(input).toSeq
  def outputs = attributes.outputs(input).toSeq
  def outgoings = attributes.outgoings(input).toSeq

  def setTags(ts: Set[Int]): InputChannel = copy(tags = ts)
  def contains(node: CompNode): Boolean = input.id == node.id
}
