package com.nicta.scoobi
package impl
package exec

import org.kiama.attribution.Attribution
import core._
import plan.mscr._
import plan.comp._
import util.UniqueId
import core.WireFormat._
import mapreducer._
/**
 * GADT representing elementary computations to perform in hadoop jobs
 */
sealed trait ExecutionNode extends CompNode {
  lazy val id = UniqueId.get

  def referencedNode: CompNode
  def sinks = referencedNode.sinks
  lazy val bridgeStore = referencedNode.bridgeStore
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
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]): TaggedMapper = referencedNode.mr.makeTaggedIdentityMapper(tags(referencedNode))
}

case class ReturnExec(rt: Ref[Return[_]]) extends ExecutionNode {
  def referencedNode = rt.n
}
case class MaterializeExec(mat: Ref[Materialize[_]], n: CompNode) extends ExecutionNode {
  def referencedNode = mat.n
}
case class OpExec(op: Ref[Op[_,_,_]], a: CompNode, b: CompNode) extends ExecutionNode {
  def referencedNode = op.n
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

  def environment(implicit sc: ScoobiConfiguration) = referencedNode.environment(sc)

  def makeTaggedReducer(tag: Int) = referencedNode.makeTaggedReducer(tag)
}

case class MscrExec(inputs: Set[InputChannel] = Set(), outputs: Set[OutputChannel] = Set()) {
  def inputChannels:  Set[InputChannelExec]  = inputs. map(_.asInstanceOf[InputChannelExec])
  def outputChannels: Set[OutputChannelExec] = outputs.map(_.asInstanceOf[OutputChannelExec])
  def channels: Set[ChannelExec] = inputChannels ++ outputChannels
  def mapperInputChannels = inputChannels.collect { case m @ MapperInputChannelExec(_) => m }

  def outputContains(node: CompNode) =
    outputChannels.exists(_.contains(node))
}

trait ChannelExec {
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob
}

sealed trait InputChannelExec extends InputChannel with ChannelExec {
  def sources = {
    Attribution.initTree(input.referencedNode)
    input.referencedNode match {
      case n: Load[_] => Seq(n.source)
      case n          => n.children.collect { case Load1(s) => s }.toSeq
    }
  }.distinct

  def input: MapperExecutionNode
  def referencedNode = input.referencedNode

  protected lazy val plan = new ExecutionPlan {}
}

/**
 * @param nodes: list of related MapperExec nodes
 */
case class MapperInputChannelExec(nodes: Seq[CompNode]) extends InputChannelExec {
  def input  = nodes.head.asInstanceOf[MapperExecutionNode]
  def inputs = nodes

  def mappers: Seq[MapperExecutionNode] = nodes.map(_.asInstanceOf[MapperExecutionNode])
  def referencedNodes: Seq[CompNode]    = mappers.map(_.referencedNode)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    mappers.map { mapper =>
      val pd = mapper.referencedNode.asInstanceOf[ParallelDo[_,_,_]]
      val env = pd.environment(sc)
      sources.foreach(source => job.addTaggedMapper(source, Some(env), mapper.makeTaggedMapper(tags)))
    }
    job
  }
}

case class BypassInputChannelExec(in: CompNode, gbk: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[MapperExecutionNode]
  def inputs = Seq(in)

  def gbkExec = gbk.asInstanceOf[GroupByKeyExec]
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    sources.foreach(source => job.addTaggedMapper(source, None, gbkExec.referencedNode.makeTaggedIdentityMapper(tags(referencedNode))))
    job
  }
}
case class StraightInputChannelExec(in: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[MapperExecutionNode]
  def inputs = Seq(in)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    val tags = plan.tags(job.mscrExec)(this)
    val mapper =  new TaggedIdentityMapper(tags(referencedNode), manifestWireFormat[Int], grouping[Int], referencedNode.asInstanceOf[DComp[_]].mr.mwf) {
      override def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit((RollingInt.get, input)) }
    }
    sources.foreach(source => job.addTaggedMapper(source, None, mapper))
    job
  }
}

sealed trait OutputChannelExec extends OutputChannel with ChannelExec {
  def sinks: Seq[Sink]
  def outputs: Seq[CompNode]
  def contains(node: CompNode) = outputs.contains(node)
  def environment: Option[CompNode]
}

case class GbkOutputChannelExec(groupByKey: CompNode,
                                flatten:    Option[CompNode]     = None,
                                combiner:   Option[CompNode]     = None,
                                reducer:    Option[CompNode]     = None,
                                sinks:      Seq[Sink] = Seq(),
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
    theReducer.map(r => job.addTaggedReducer(sinks, Some(r.environment(sc)), r.makeTaggedReducer(tag))).orElse {
      theCombiner.map(c => job.addTaggedReducer(sinks, None, c.makeTaggedReducer(tag))).orElse {
        Some(job.addTaggedReducer(sinks, None, theGroupByKey.makeTaggedReducer(tag)))
      }
    }
    job
  }
  def environment: Option[CompNode] = theReducer.map(_.referencedNode.env)
}

case class FlattenOutputChannelExec(out: CompNode, sinks: Seq[Sink] = Seq(), tag: Int = 0) extends OutputChannelExec {
  lazy val theFlatten = out.asInstanceOf[FlattenExec]

  override def equals(a: Any) = a match {
    case o: FlattenOutputChannelExec => o.out == out
    case _                           => false
  }
  def outputs = Seq(out)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, theFlatten.referencedNode.makeTaggedReducer(tag))

  def environment: Option[CompNode] = None
}
case class BypassOutputChannelExec(out: CompNode, sinks: Seq[Sink] = Seq(), tag: Int = 0) extends OutputChannelExec {
  lazy val theParallelDo = out.asInstanceOf[MapperExec]

  override def equals(a: Any) = a match {
    case o: BypassOutputChannelExec => o.out == out
    case _                          => false
  }
  def outputs = Seq(out)
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, theParallelDo.referencedNode.makeTaggedIdentityReducer(tag))

  def environment: Option[CompNode] = Some(theParallelDo.referencedNode.env)

}