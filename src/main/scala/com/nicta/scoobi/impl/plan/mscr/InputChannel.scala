package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable
import core._
import comp._
import util.UniqueId
import collection.IdSet
import exec.MapReduceJob
import mapreducer.TaggedIdentityMapper
import core.WireFormat._
import comp.Load
import scala.Some
import comp.Combine
import comp.GroupByKey
import CompNodes._
import shapeless.Nat._0

trait Channel extends Attributable {
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob
}

/** ADT for MSCR input channels. */
trait InputChannel extends Channel {
  protected val attributes = new CompNodes {}
  
  lazy val id: Int = UniqueId.get
  def inputs: Seq[CompNode]
  def outputs: Seq[CompNode]
  def sourceNodes: Seq[CompNode] = incomings.filter(isSourceNode)
  def incomings: Seq[CompNode]
  def outgoings: Seq[CompNode]

  def setTags(ts: CompNode => Set[Int]): InputChannel
  def tags: CompNode => Set[Int]
  def nodesTags: Set[Int] = nodes.flatMap(tags).toSet
  def nodes: Seq[CompNode]
  def contains(node: CompNode): Boolean = nodes.exists(_.id == node.id)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob

  lazy val sources = {
    nodes.head match {
      case n: Load[_]         => Seq(n.source)
      case n: GroupByKey[_,_] => Seq(n.bridgeStore).flatten
      case n: Combine[_,_]    => Seq(n.bridgeStore).flatten
      case n                  => n.children.asNodes.flatMap {
        case ld: Load[_] => Some(ld.source)
        case other       => other.bridgeStore
      }.toSeq
    }
  }

}

case class MapperInputChannel(var parDos: Set[ParallelDo[_,_,_]], gbk: Option[GroupByKey[_,_]] = None, tags: CompNode => Set[Int] = (_:CompNode) => Set(0)) extends InputChannel {
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

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = parDos.toSeq

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    parDos.map { pd =>
      job.addTaggedMapper(sources.head, pd.environment(sc),
                          gbk.map(g => pd.makeTaggedMapper(g, tags(pd))).getOrElse(pd.makeTaggedMapper(tags(pd))))
    }
    job
  }
}
object MapperInputChannel {
  def apply(pd: ParallelDo[_,_,_]*): MapperInputChannel = new MapperInputChannel(IdSet(pd:_*))
}

case class IdInputChannel(input: CompNode, gbk: Option[GroupByKey[_,_]] = None, tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def inputs = Seq(input)
  def incomings = attributes.incomings(input).toSeq
  def outputs = attributes.outputs(input).toSeq
  def outgoings = attributes.outgoings(input).toSeq

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = Seq(input)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    gbk.map(g => sources.foreach(source => job.addTaggedMapper(source, None, g.makeTaggedIdentityMapper(tags(input)))))
    job
  }
}

case class StraightInputChannel(input: CompNode, tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: StraightInputChannel => i.input.id == input.id
    case _                       => false
  }

  def inputs = attributes.inputs(input).toSeq
  def incomings = attributes.incomings(input).toSeq
  def outputs = attributes.outputs(input).toSeq
  def outgoings = attributes.outgoings(input).toSeq

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = Seq(input)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val mapper =  new TaggedIdentityMapper(tags(input), manifestWireFormat[Int], grouping[Int], input.asInstanceOf[DComp[_]].mr.mwf) {
      override def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit((RollingInt.get, input)) }
    }
    sources.foreach(source => job.addTaggedMapper(source, None, mapper))
    job
  }

}
