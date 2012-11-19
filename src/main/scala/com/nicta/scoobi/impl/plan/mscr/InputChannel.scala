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
  lazy val id: Int = UniqueId.get
  def inputs: Seq[CompNode]
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
  def inputs = parDos.toSeq.flatMap(pd => Seq(pd.in, pd.env))
}
object MapperInputChannel {
  def apply(pd: ParallelDo[_,_,_]*): MapperInputChannel = new MapperInputChannel(IdSet(pd:_*))
}
case class IdInputChannel(input: Option[CompNode], gbk: GroupByKey[_,_]) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.map(_.id).getOrElse(i.gbk.id) == input.map(_.id).getOrElse(gbk.id)
    case _                 => false
  }

  def inputs = input match {
    case Some(pd: ParallelDo[_,_,_]) => Seq(pd.in)
    case Some(cb: Combine[_,_])      => Seq(cb.in)
    case None                        => Seq(gbk.in)
  }
}
case class StraightInputChannel(input: CompNode) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _                       => false
  }
  def inputs = input match {
    case fl: Flatten[_] => fl.ins
    case _              => Seq()
  }
}
