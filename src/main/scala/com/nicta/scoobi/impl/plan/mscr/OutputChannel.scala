package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._
import util._
import CompNodes._
import exec.MapReduceJob

/** ADT for MSCR output channels. */
trait OutputChannel extends Channel {
  protected val attributes = new CompNodes {}

  lazy val id: Int = UniqueId.get

  /** sinks for this output channel */
  def sinks: Seq[Sink]
  /** @return the nodes which are part of this channel */
  def nodes: Seq[CompNode]
  def contains(node: CompNode) = nodes.contains(node)
  def outgoings = nodes.flatMap(attributes.outgoings).toSeq
  def incomings = nodes.flatMap(attributes.incomings).toSeq
  def sourceNodes: Seq[CompNode] = incomings.filter(isSourceNode)
  def sinkNodes: Seq[CompNode] = outgoings.filter(isSinkNode)
  def environment: Option[CompNode]
  def tag: Int
  def setTag(t: Int): OutputChannel
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob

}

trait MscrOutputChannel extends OutputChannel {
  def sinks = if (nodeSinks.isEmpty) bridgeStore.toSeq else nodeSinks
  protected def nodeSinks: Seq[Sink]
  def bridgeStore: Option[Bridge]
  def output: CompNode
  def environment: Option[CompNode]
}
case class GbkOutputChannel(groupByKey:   GroupByKey[_,_],
                            flatten:  Option[Flatten[_]]        = None,
                            combiner: Option[Combine[_,_]]      = None,
                            reducer:  Option[ParallelDo[_,_,_]] = None,
                            tag: Int = 0) extends MscrOutputChannel {

  override def toString =
    Seq(Some(groupByKey),
        flatten .map(n => "flatten  = "+n.toString),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")

  override def equals(a: Any) = a match {
    case o: GbkOutputChannel => o.groupByKey.id == groupByKey.id
    case _                   => false
  }

  def nodes: Seq[CompNode] = Seq[CompNode](groupByKey) ++ flatten.toSeq ++ combiner.toSeq ++ reducer.toSeq
  def setTag(t: Int) = copy(tag = t)
  /** @return the output node of this channel */
  def output = reducer.map(r => r: CompNode).orElse(combiner).orElse(flatten).getOrElse(groupByKey)

  def nodeSinks = nodes.flatMap(_.sinks)

  lazy val bridgeStore =
     (reducer: Option[CompNode]).
      orElse(combiner  ).
      getOrElse(groupByKey).bridgeStore

  def environment: Option[CompNode] = reducer.map(_.env)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob = {
    combiner.map(c => job.addTaggedCombiner(c.makeTaggedCombiner(tag)))

    // if there is a reducer node, use it as the tagged reducer
    // otherwise use the combiner node if there is one
    // and finally default to the GroupByKey node
    combiner.map { c =>
      reducer.map { r =>
        Some(job.addTaggedReducer(sinks, r.environment(sc), c.makeTaggedReducer(tag, r.dofn)))
      }.orElse(Some(job.addTaggedReducer(sinks, None, c.makeTaggedReducer(tag))))
    }.orElse {
      reducer.map { r =>
        Some(job.addTaggedReducer(sinks, r.environment(sc), r.makeTaggedReducer(tag)))
      }
    }.getOrElse(job.addTaggedReducer(sinks, None, groupByKey.makeTaggedReducer(tag)))
    job
  }


}

case class BypassOutputChannel(output: ParallelDo[_,_,_], tag: Int = 0) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }
  def setTag(t: Int) = copy(tag = t)
  def nodes = Seq(output)
  def nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
  def environment: Option[CompNode] = Some(output.env)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, output.makeTaggedIdentityReducer(tag))

}

case class FlattenOutputChannel(output: Flatten[_], tag: Int = 0) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannel => o.output.id == output.id
    case _ => false
  }
  def nodes: Seq[CompNode] = Seq(output)
  def setTag(t: Int) = copy(tag = t)
  def nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
  def environment: Option[CompNode] = None
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, output.makeTaggedIdentityReducer(tag))

}

object Channels extends control.ImplicitParameters {
  /** @return a sequence of distinct mapper input channels */
  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.parDos.map(_.id).toSet, in)).toMap.values.toSeq

  /** @return a sequence of distinct group by key output channels */
  def distinct(out: Seq[GbkOutputChannel])(implicit p: ImplicitParam): Seq[GbkOutputChannel] =
    out.map(o => (o.groupByKey.id, o)).toMap.values.toSeq

  /** @return a sequence of distinct bypass output channels */
  def distinct(out: Seq[BypassOutputChannel])(implicit p1: ImplicitParam1, p2: ImplicitParam2): Seq[BypassOutputChannel] =
    out.map(o => (o.output.id, o)).toMap.values.toSeq
}
