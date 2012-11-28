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
import CompNodes._
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
case class LoadExec(load: Ref[Load[_]]) extends MapperExecutionNode {
  def referencedNode = load.n
  def makeTaggedMapper(tags: Map[CompNode, Set[Int]]): TaggedMapper = referencedNode.mr.makeTaggedIdentityMapper(tags(referencedNode))
}
case class GroupByKeyExec(gbk: Ref[GroupByKey[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = gbk.n
  def makeTaggedReducer(tag: Int) = referencedNode.makeTaggedReducer(tag)
}
case class CombineExec(cb: Ref[Combine[_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = cb.n
  def makeTaggedCombiner(tag: Int)                      = referencedNode.makeTaggedCombiner(tag)
  def makeTaggedReducer(tag: Int)                       = referencedNode.makeTaggedReducer(tag)
  def makeTaggedReducer(tag: Int, dofn: EnvDoFn[_,_,_]) = referencedNode.makeTaggedReducer(tag, dofn)
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
/** specialised ParallelDo to be translated to a Hadoop Reducer class, following a Gbk */
case class GbkReducerExec(pd: Ref[ParallelDo[_,_,_]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
  def dofn           = referencedNode.dofn

  def environment(implicit sc: ScoobiConfiguration) = referencedNode.environment(sc)
  def makeTaggedReducer(tag: Int)             = referencedNode.makeTaggedReducer(tag)
}

case class MscrExec(inputs: Seq[InputChannel] = Seq(), outputs: Seq[OutputChannel] = Seq()) {
  def inputChannels:  Seq[InputChannelExec]  = inputs. map(_.asInstanceOf[InputChannelExec])
  def outputChannels: Seq[OutputChannelExec] = outputs.map(_.asInstanceOf[OutputChannelExec])
  def channels: Seq[ChannelExec] = inputChannels ++ outputChannels
  def mapperInputChannels = inputChannels.collect { case m @ MapperInputChannelExec(_) => m }

  def outputContains(node: CompNode) =
    outputChannels.exists(_.contains(node))
}

trait ChannelExec {
  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration): MapReduceJob
}

sealed trait InputChannelExec extends InputChannel with ChannelExec {
  def tags: CompNode => Set[Int] = (c: CompNode) => Set[Int]()
  def setTags(tags: (CompNode) => Set[Int]) = this
  def incomings = Seq[CompNode]()
  def outputs = Seq[CompNode]()
  def outgoings = Seq[CompNode]()

  def input: ExecutionNode
  def referencedNode = input.referencedNode

  protected lazy val plan = new ExecutionPlan {}
  def nodes = Seq[CompNode]()

}

/**
 * @param nodes: list of related MapperExec nodes
 */
case class MapperInputChannelExec(override val nodes: Seq[CompNode]) extends InputChannelExec {
  def input  = nodes.head.asInstanceOf[MapperExecutionNode]
  def inputs = nodes

  def mappers: Seq[MapperExecutionNode] = nodes.map(_.asInstanceOf[MapperExecutionNode])
  def referencedNodes: Seq[CompNode]    = mappers.map(_.referencedNode)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    job
  }
}

case class BypassInputChannelExec(in: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[ExecutionNode]
  def inputs = Seq(input)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    job
  }
}
case class StraightInputChannelExec(in: CompNode) extends InputChannelExec {
  def input = in.asInstanceOf[MapperExecutionNode]
  def inputs = Seq(in)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) = {
    job
  }
}

sealed trait OutputChannelExec extends OutputChannel with ChannelExec {
  def sinks: Seq[Sink]
  def outputs: Seq[CompNode]
  def environment: Option[CompNode]
  def nodes: Seq[CompNode] = Seq()
  def tag: Int
  def setTag(t: Int): OutputChannelExec
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
    theCombiner.map { c =>
      theReducer.map { r =>
        Some(job.addTaggedReducer(sinks, r.environment(sc), c.makeTaggedReducer(tag, r.dofn)))
      }.orElse(Some(job.addTaggedReducer(sinks, None, c.makeTaggedReducer(tag))))
    }.orElse {
      theReducer.map { r =>
        Some(job.addTaggedReducer(sinks, r.environment(sc), r.makeTaggedReducer(tag)))
      }
    }.getOrElse(job.addTaggedReducer(sinks, None, theGroupByKey.makeTaggedReducer(tag)))
    job
  }
  def environment: Option[CompNode] = theReducer.map(_.referencedNode.env)
  def setTag(t: Int) = copy(tag = t)

}

case class FlattenOutputChannelExec(out: CompNode, sinks: Seq[Sink] = Seq(), tag: Int = 0) extends OutputChannelExec {
  lazy val theFlatten = out.asInstanceOf[FlattenExec]

  override def equals(a: Any) = a match {
    case o: FlattenOutputChannelExec => o.out == out
    case _                           => false
  }
  def outputs = Seq(out)

  def configure(job: MapReduceJob)(implicit sc: ScoobiConfiguration) =
    job.addTaggedReducer(sinks.toList, None, theFlatten.referencedNode.makeTaggedIdentityReducer(tag))

  def environment: Option[CompNode] = None
  def setTag(t: Int) = copy(tag = t)
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
  def setTag(t: Int) = copy(tag = t)
}