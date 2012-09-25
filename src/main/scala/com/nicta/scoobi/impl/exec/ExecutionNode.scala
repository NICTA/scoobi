package com.nicta.scoobi
package impl
package exec

import plan.comp._
import application.ScoobiConfiguration
import plan.graph.{OutputChannel, InputChannel}
import io.DataSource

/**
 * GADT representing elementary computations to perform in hadoop jobs
 */
sealed trait ExecutionNode extends CompNode {
  def referencedNode: CompNode
}

case class Ref[T <: CompNode](n: T)
case class LoadExec[T](load: Ref[Load[T]]) extends ExecutionNode {
  def referencedNode = load.n
}
case class GroupByKeyExec[K, V](gbk: Ref[GroupByKey[K, V]], n: CompNode) extends ExecutionNode {
  def referencedNode = gbk.n
}
case class CombineExec[K, V](cb: Ref[Combine[K, V]], n: CompNode) extends ExecutionNode {
  def referencedNode = cb.n
}
case class FlattenExec[T](flatten: Ref[Flatten[T]], ins: Seq[CompNode]) extends ExecutionNode {
  def referencedNode = flatten.n
}

case class ReturnExec[T](rt: Ref[Return[T]]) extends ExecutionNode {
  def referencedNode = rt.n
  def env(implicit sc: ScoobiConfiguration) = Env(rt.n.wf, sc)
}
case class MaterializeExec[T](mat: Ref[Materialize[T]], n: CompNode) extends ExecutionNode {
  def referencedNode = mat.n
  def env(implicit sc: ScoobiConfiguration) = Env(mat.n.wf, sc)
}
case class OpExec[A, B, C](op: Ref[Op[A, B, C]], a: CompNode, b: CompNode) extends ExecutionNode {
  def referencedNode = op.n
  def env(implicit sc: ScoobiConfiguration) = Env(op.n.wf, sc)
}

/** specialised ParallelDo to be translated to a Hadoop Mapper class */
case class MapperExec[A, B, E](pd: Ref[ParallelDo[A, B, E]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
}
/** specialised ParallelDo to be translated to a Hadoop Reducer class */
case class ReducerExec[A, B, E](pd: Ref[ParallelDo[A, B, E]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
}
/** specialised ParallelDo to be translated to a Hadoop Reducer class, following a Gbk */
case class GbkReducerExec[A, B, E](pd: Ref[ParallelDo[A, B, E]], n: CompNode) extends ExecutionNode {
  def referencedNode = pd.n
}

case class MscrExec(inputs: Set[InputChannel] = Set(), outputs: Set[OutputChannel] = Set())

sealed trait InputChannelExec extends InputChannel {
  def source: DataSource[_,_,_] = input.referencedNode.dataSource
  def input: ExecutionNode
}

/**
 * @param nodes: list of related MapperExec nodes
 */
case class MapperInputChannelExec(nodes: Seq[CompNode]) extends InputChannelExec {
  def input: ExecutionNode = nodes.head.asInstanceOf[ExecutionNode]
  //  def inputEnv: ExecutionNode= nodes.head.env
}

/**
 * @param in
 */
case class BypassInputChannelExec(in: CompNode) extends InputChannelExec {
  def input: ExecutionNode = in.asInstanceOf[ExecutionNode]
}
/**
 * @param in
 */
case class StraightInputChannelExec(in: CompNode) extends InputChannelExec {
  def input: ExecutionNode = in.asInstanceOf[ExecutionNode]
}

sealed trait OutputChannelExec extends OutputChannel
case class GbkOutputChannelExec(groupByKey: CompNode,
                                flatten:    Option[CompNode] = None,
                                combiner:   Option[CompNode] = None,
                                reducer:    Option[CompNode] = None) extends OutputChannelExec

case class FlattenOutputChannelExec(in: CompNode) extends OutputChannelExec
case class BypassOutputChannelExec(in: CompNode) extends OutputChannelExec
