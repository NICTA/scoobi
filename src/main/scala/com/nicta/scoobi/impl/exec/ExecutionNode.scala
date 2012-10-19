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
trait MapperExecutionNode extends ExecutionNode {
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]): TaggedMapper
}

case class Ref[T <: CompNode](n: T)
case class LoadExec(load: Ref[Load[_]]) extends ExecutionNode {
  def referencedNode = load.n
}
case class GroupByKeyExec(gbk: Ref[GroupByKey[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = gbk.n
  def makeTaggedReducer(tag: Int) = referencedNode.makeTaggedReducer(tag)
}
case class CombineExec(cb: Ref[Combine[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = cb.n
  def makeTaggedCombiner(tag: Int) = referencedNode.makeTaggedCombiner(tag)
  def makeTaggedReducer(tag: Int)  = referencedNode.makeTaggedReducer(tag)
}
case class FlattenExec(flatten: Ref[Flatten[_]], ins: Seq[CompNode]) extends MapperExecutionNode {
  def referencedNode = flatten.n
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]): TaggedMapper = referencedNode.makeStraightTaggedIdentityMapper(tags(referencedNode))
}

case class ReturnExec(rt: Ref[Return[_]]) extends ExecutionNode {
  def referencedNode = rt.n
  def env(implicit sc: ScoobiConfiguration) = Env(rt.n.wf)(sc)
}
case class MaterializeExec(mat: Ref[Materialize[_]], n: CompNode) extends ExecutionNode {
  def referencedNode = mat.n
  def env(implicit sc: ScoobiConfiguration) = Env(mat.n.wf)(sc)
}
case class OpExec(op: Ref[Op[_,_,_]], a: CompNode, b: CompNode) extends ExecutionNode {
  def referencedNode = op.n
  def env(implicit sc: ScoobiConfiguration) = Env(op.n.wf)(sc)
}

/** specialised ParallelDo to be translated to a Hadoop Mapper class */
case class MapperExec(pd: Ref[ParallelDo[_,_,_]], n: CompNode) extends MapperExecutionNode {
  def referencedNode = pd.n
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]) = referencedNode.makeTaggedMapper(tags(referencedNode))
}
/** specialised ParallelDo followed by a GroupByKey to be translated to a Hadoop Mapper class */
case class GbkMapperExec(pd: Ref[ParallelDo[_,_,_]], gbk: Ref[GroupByKey[_,_]], n: CompNode) extends MapperExecutionNode {
  def referencedNode = pd.n
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]) = referencedNode.makeTaggedMapper(gbk.n, tags(referencedNode))
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
  def env(implicit sc: ScoobiConfiguration) = Env(referencedNode.wfe)(sc)
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
  def input: MapperExecutionNode
  def referencedNode = input.referencedNode

  protected lazy val plan = new ExecutionPlan {}
}

/**
 * @param nodes: list of related MapperExec nodes
 */
case class MapperInputChannelExec(nodes: Seq[CompNode]) extends InputChannelExec {
  def input                             = nodes.head.asInstanceOf[MapperExecutionNode]
  def mappers: Seq[MapperExecutionNode] = nodes.map(_.asInstanceOf[MapperExecutionNode])
  def referencedNodes: Seq[CompNode]    = mappers.map(_.referencedNode)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    mappers.map { mapper =>
      val pd = mapper.referencedNode.asInstanceOf[ParallelDo[_,_,_]]
      val env = Env(pd.wfe)
      job.addTaggedMapper(source, Some(env), mapper.makeTaggedMapper(tags))
    }
    job
  }
}

case class BypassInputChannelExec(in: CompNode, gbk: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[MapperExecutionNode]
  def gbkExec = gbk.asInstanceOf[GroupByKeyExec]
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    job.addTaggedMapper(source, None, gbkExec.referencedNode.makeTaggedIdentityMapper(tags(referencedNode)))
    job
  }
}
case class StraightInputChannelExec(in: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[MapperExecutionNode]
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    job.addTaggedMapper(source, None, referencedNode.asInstanceOf[DComp[_,_]].makeStraightTaggedIdentityMapper(tags(referencedNode)))
  }
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

  lazy val theCombiner   = combiner.asInstanceOf[Option[CombineExec]]
  lazy val theReducer    = reducer.asInstanceOf[Option[GbkReducerExec]]
  lazy val theGroupByKey = groupByKey.asInstanceOf[GroupByKeyExec]

  override def equals(a: Any) = a match {
    case o: GbkOutputChannelExec => o.groupByKey == groupByKey
    case _                       => false
  }

  def outputs = Seq(Seq(groupByKey), flatten.toSeq, combiner.toSeq, reducer.toSeq).flatten

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    theCombiner.map(c => job.addTaggedCombiner(c.makeTaggedCombiner(tag)))

    // if there is a reducer node, use it as the tagged reducer
    // otherwise use the combiner node if there is one
    // and finally default to the GroupByKey node
    theReducer.map(r => job.addTaggedReducer(sinks.toList, Some(r.env), r.makeTaggedReducer(tag))).orElse {
      theCombiner.map(c => job.addTaggedReducer(sinks.toList, None, c.makeTaggedReducer(tag))).orElse {
        Some(job.addTaggedReducer(sinks.toList, None, theGroupByKey.makeTaggedReducer(tag)))
      }
    }
    job
  }
}

case class FlattenOutputChannelExec(out: CompNode, sinks: Seq[DataSink[_,_,_]] = Seq(), tag: Int = 0) extends OutputChannelExec {
  lazy val theFlatten = out.asInstanceOf[FlattenExec]

  override def equals(a: Any) = a match {
    case o: FlattenOutputChannelExec => o.out == out
    case _                           => false
  }
  def outputs = Seq(out)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, theFlatten.referencedNode.makeTaggedReducer(tag))
}
case class BypassOutputChannelExec(out: CompNode, sinks: Seq[DataSink[_,_,_]] = Seq(), tag: Int = 0) extends OutputChannelExec {
  lazy val theParallelDo = out.asInstanceOf[MapperExec]

  override def equals(a: Any) = a match {
    case o: BypassOutputChannelExec => o.out == out
    case _                          => false
  }
  def outputs = Seq(out)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, theParallelDo.referencedNode.makeTaggedReducer(tag))
}