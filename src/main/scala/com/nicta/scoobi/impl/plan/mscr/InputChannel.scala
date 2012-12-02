package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable
import core._
import comp._
import util.UniqueId
import exec.MapReduceJob
import mapreducer.TaggedIdentityMapper
import core.WireFormat._
import comp.GroupByKey
import CompNodes._
import scalaz.Equal

trait Channel extends Attributable {
  def configure[T <: MscrJob](job: T)(implicit sc: ScoobiConfiguration): T
}

/** ADT for MSCR input channels. */
trait InputChannel extends Channel {
  protected val attributes = new CompNodes {}
  
  lazy val id: Int = UniqueId.get
  override def equals(a: Any) = a match {
    case i: InputChannel => i.id == id
    case other           => false
  }
  override def hashCode = id.hashCode

  def inputs: Seq[CompNode]
  def outputs: Seq[CompNode]
  def sourceNodes: Seq[CompNode] = incomings.filter(isSourceNode)
  def incomings: Seq[CompNode]
  def outgoings: Seq[CompNode]

  def setTags(ts: CompNode => Set[Int]): InputChannel
  def tags: CompNode => Set[Int]
  def nodesTags: Set[Int] = nodes.flatMap(tags).toSet
  def nodes: Seq[CompNode]
  def contains(node: CompNode): Boolean = nodes.contains(node)

  def sources: InputChannel => Seq[Source]
}

case class MapperInputChannel(parDos: Seq[ParallelDo[_,_,_]],
                              sources: InputChannel => Seq[Source],
                              gbk: Option[GroupByKey[_,_]] = None,
                              tags: CompNode => Set[Int] = (_:CompNode) => Set(0)) extends InputChannel {
  override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"
  override def equals(a: Any) = a match {
    case i: MapperInputChannel => i.parDos.map(_.id) == parDos.map(_.id)
    case _                     => false
  }

  def inputs = parDos.flatMap(pd => attributes.inputs(pd))
  def incomings = parDos.flatMap(pd => attributes.incomings(pd))

  def outputs = parDos.flatMap(pd => attributes.outputs(pd)).toSeq
  def outgoings = parDos.flatMap(pd => attributes.outgoings(pd))

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = parDos

  def configure[T <: MscrJob](job: T)(implicit sc: ScoobiConfiguration): T = {
    parDos.map { pd =>
      job.addTaggedMapper(sources(this).head, pd.environment(sc),
                          gbk.map(g => pd.makeTaggedMapper(g, tags(pd))).getOrElse(pd.makeTaggedMapper(tags(pd))))
    }
    job
  }
}
object MapperInputChannel {
  def create(pd: Seq[ParallelDo[_,_,_]], sources: InputChannel => Seq[Source]): MapperInputChannel = new MapperInputChannel(pd, sources)
}

case class IdInputChannel(input: CompNode,
                          sources: InputChannel => Seq[Source] = (in: InputChannel) => Seq(),
                          gbk: Option[GroupByKey[_,_]] = None,
                          tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
  override def equals(a: Any) = a match {
    case i: IdInputChannel => i.input.id == input.id
    case _                 => false
  }

  def inputs = Seq(input)
  def incomings = attributes.incomings(input)
  def outputs = attributes.outputs(input)
  def outgoings = attributes.outgoings(input)

  def setTags(ts: CompNode => Set[Int]): InputChannel = copy(tags = ts)
  def nodes: Seq[CompNode] = Seq(input)

  def configure[T <: MscrJob](job: T)(implicit sc: ScoobiConfiguration): T = {
    gbk.map(g => sources(this).foreach(source => job.addTaggedMapper(source, None, g.makeTaggedIdentityMapper(tags(input)))))
    job
  }
}

case class StraightInputChannel(input: CompNode,
                                sources: InputChannel => Seq[Source] = (in: InputChannel) => Seq(),
                                tags: CompNode => Set[Int] = (c: CompNode) => Set(0)) extends InputChannel {
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

  def configure[T <: MscrJob](job: T)(implicit sc: ScoobiConfiguration): T = {
  val mapper =  new TaggedIdentityMapper(tags(input), manifestWireFormat[Int], grouping[Int], input.asInstanceOf[DComp[_]].mr.mwf) {
      override def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit((RollingInt.get, input)) }
    }
    sources(this).foreach(source => job.addTaggedMapper(source, None, mapper))
    job
  }

}

object InputChannel {
  implicit def inputChannelEqual = new Equal[InputChannel] {
    def equal(a1: InputChannel, a2: InputChannel) = a1.id == a2.id
  }
}
