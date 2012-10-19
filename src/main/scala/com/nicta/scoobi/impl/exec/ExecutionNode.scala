package com.nicta.scoobi
package impl
package exec

import plan.comp._
import application.ScoobiConfiguration
import plan.graph.{OutputChannel, InputChannel}
import io.{DataSink, DataSource}
import org.apache.hadoop.mapreduce.Job
import com.nicta.scoobi.core.Emitter
import com.nicta.scoobi.core.Grouping
import com.nicta.scoobi.core.WireFormat

/**
 * GADT representing elementary computations to perform in hadoop jobs
 */
sealed trait ExecutionNode extends CompNode {
  def referencedNode: CompNode
}

case class Ref[T <: CompNode](n: T)
case class LoadExec(load: Ref[Load[_]]) extends ExecutionNode {
  def referencedNode = load.n
}
case class GroupByKeyExec(gbk: Ref[GroupByKey[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = gbk.n
}
case class CombineExec(cb: Ref[Combine[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = cb.n
  def makeTaggedCombiner(tag: Int) = referencedNode.makeTaggedCombiner(tag)
}
case class FlattenExec(flatten: Ref[Flatten[_]], ins: Seq[CompNode]) extends ExecutionNode {
  def referencedNode = flatten.n
}

case class ReturnExec(rt: Ref[Return[_]]) extends ExecutionNode {
  def referencedNode = rt.n
  def env(implicit sc: ScoobiConfiguration) = Env(rt.n.wf, sc)
}
case class MaterializeExec(mat: Ref[Materialize[_]], n: CompNode) extends ExecutionNode {
  def referencedNode = mat.n
  def env(implicit sc: ScoobiConfiguration) = Env(mat.n.wf, sc)
}
case class OpExec(op: Ref[Op[_,_,_]], a: CompNode, b: CompNode) extends ExecutionNode {
  def referencedNode = op.n
  def env(implicit sc: ScoobiConfiguration) = Env(op.n.wf, sc)
}

/** specialised ParallelDo to be translated to a Hadoop Mapper class */
case class MapperExec(pd: Ref[ParallelDo[_,_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
}
/** specialised ParallelDo to be translated to a Hadoop Reducer class */
case class ReducerExec(pd: Ref[ParallelDo[_,_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
}
/** specialised ParallelDo to be translated to a Hadoop Reducer class, following a Gbk */
case class GbkReducerExec(pd: Ref[ParallelDo[_,_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
  def dofn           = referencedNode.dofn
  
  def makeTaggedReducer(tag: Int) = referencedNode.makeTaggedReducer(tag)
  def env(implicit sc: ScoobiConfiguration) = Env(referencedNode.wfe, sc)
}

case class MscrExec(inputs: Set[InputChannel] = Set(), outputs: Set[OutputChannel] = Set()) {
  def inputChannels:  Set[InputChannelExec]  = inputs. map(_.asInstanceOf[InputChannelExec])
  def outputChannels: Set[OutputChannelExec] = outputs.map(_.asInstanceOf[OutputChannelExec])
  def channels: Set[ChannelExec] = inputChannels ++ outputChannels
  def mapperInputChannels = inputChannels.collect { case m @ MapperInputChannelExec(_) => m }
}

trait ChannelExec {
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob
}

sealed trait InputChannelExec extends InputChannel with ChannelExec {
  def source: DataSource[_,_,_] = input.referencedNode.dataSource
  def input: ExecutionNode
  def referencedNode = input.referencedNode
}

/**
 * @param nodes: list of related MapperExec nodes
 */
case class MapperInputChannelExec(nodes: Seq[CompNode]) extends InputChannelExec {
  def input: ExecutionNode = nodes.head.asInstanceOf[ExecutionNode]
  def inputs: Seq[ExecutionNode] = nodes.map(_.asInstanceOf[ExecutionNode])
  def referencedNodes: Seq[CompNode] = inputs.map(_.referencedNode)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = job
}

case class BypassInputChannelExec(in: CompNode) extends InputChannelExec {
  def input: ExecutionNode = in.asInstanceOf[ExecutionNode]
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = job
}
case class StraightInputChannelExec(in: CompNode) extends InputChannelExec {
  def input: ExecutionNode = in.asInstanceOf[ExecutionNode]
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = job
}

sealed trait OutputChannelExec extends OutputChannel with ChannelExec {
  def sinks: Seq[DataSink[_,_,_]]
  def outputs: Seq[CompNode]
}

case class GbkOutputChannelExec(groupByKey: CompNode,
                                flatten:    Option[CompNode]     = None,
                                combiner:   Option[CompNode]     = None,
                                reducer:    Option[CompNode]     = None,
                                sinks:      Seq[DataSink[_,_,_]] = Seq(),
                                tag:        Int = 0) extends OutputChannelExec {

  val theCombiner = combiner.asInstanceOf[Option[CombineExec]]
  val theReducer  = reducer.asInstanceOf[Option[GbkReducerExec]]
  
  override def equals(a: Any) = a match {
    case o: GbkOutputChannelExec => o.groupByKey == groupByKey
    case _                       => false
  }

  def outputs = Seq(Seq(groupByKey), flatten.toSeq, combiner.toSeq, reducer.toSeq).flatten

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    theCombiner.map(c => job.addTaggedCombiner(c.makeTaggedCombiner(tag)))
    theReducer .map(r => job.addTaggedReducer(sinks.toList, Some(r.env), r.makeTaggedReducer(tag)))
    job
  }
}

case class FlattenOutputChannelExec(out: CompNode, sinks: Seq[DataSink[_,_,_]] = Seq(), tag: Int = 0) extends OutputChannelExec {
  override def equals(a: Any) = a match {
    case o: FlattenOutputChannelExec => o.out == out
    case _                           => false
  }
  def outputs = Seq(out)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = job
}
case class BypassOutputChannelExec(out: CompNode, sinks: Seq[DataSink[_,_,_]] = Seq(), tag: Int = 0) extends OutputChannelExec {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannelExec => o.out == out
    case _                          => false
  }
  def outputs = Seq(out)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = job
}