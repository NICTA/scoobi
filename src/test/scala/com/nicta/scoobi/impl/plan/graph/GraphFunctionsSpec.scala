package com.nicta.scoobi
package impl
package plan
package graph

import org.specs2.specification.Scope
import comp.{Return, CompNode, CompNodeFactory}
import collection._
import testing.mutable.UnitSpecification

class GraphFunctionsSpec extends UnitSpecification {

  "the inputs of a node are its children" >> new nodes {
    val load0 = load
    val cb1 = cb(load0)
    (cb1 -> inputs) ==== IdSet(load0)
  }

  "the ancestors of a node are all its direct parents" >> new nodes {
    (pd1 -> ancestors) ==== IdSet(fl1, mat1)
    (l1 -> ancestors)  ==== IdSet(pd1, fl1, mat1)
  }
  "the parents of a node are all the nodes having this node in their descendents" >> new nodes {
    (pd1 -> parents) ==== IdSet(fl1, gbk1, mat1)
    (l1 -> parents) ==== IdSet(pd1, fl1, gbk1, mat1)
  }
  "the descendents of a node are the recursive list of all children" >> new nodes {
    (mat1 -> descendents) ==== IdSet(l1, pd1.env, pd1, gbk1, fl1)
  }
  "the descendents must be collected along all paths of the graph" >> new factory {
    val ld1 = load
    val (pd1, pd2) = (pd(ld1), pd(ld1))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    val fl1 = flatten(gbk1, gbk2)

    (gbk1 -> descendents) ==== IdSet(pd1, ld1, pd1.env)
    (fl1-> descendents)   ==== IdSet(gbk1, gbk2, pd1, pd2, ld1, pd1.env, pd2.env)
  }
  "a node can be reached from another one if it is in the list of its descendents" >> new nodes {
    (fl1 -> canReach(l1)) must beTrue
  }
  "the outputs of a node are all its direct parents" >> new nodes {
    (pd1 -> outputs) ==== IdSet(fl1, gbk1)
  }
  "2 nodes are siblings if they share the same input" >> new factory {
    val load0 = load
    val pd1 = pd(load0)
    val pd2 = pd(load0)
    val graph = flatten(pd1, pd2)

    (pd1 -> siblings) ==== IdSet(pd2)
  } newp

  "CompNode functions".newp

  "2 gbks are related if" >> {
    "they share the same ParallelDo input" >> new factory {
      val pd1 = pd(load)
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd1))
      val fl1 = flatten(gbk1, gbk2)

      (gbk1 -> relatedGbks) ==== IdSet(gbk2)
    }
    "they have ParallelDos inputs which are siblings" >> new factory {
      val ld1 = load
      val (pd1, pd2) = (pd(ld1), pd(ld1))
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
      val fl1 = flatten(gbk1, gbk2)

      (gbk1 -> relatedGbks) ==== IdSet(gbk2)
    }
    "they have Flatten nodes inputs with ParallelDos inputs which are siblings" >> new factory {
      val ld1 = load
      val (pd1, pd2) = (pd(ld1), pd(ld1))
      val (gbk1, gbk2) = (gbk(flatten(pd1)), gbk(flatten(pd2)))
      val fl1 = flatten(gbk1, gbk2)

      (gbk1 -> relatedGbks) ==== IdSet(gbk2)
    }
    "they have Flatten nodes inputs with ParallelDos inputs which are siblings - mixed version" >> new factory {
      val ld1 = load
      val (pd1, pd2) = (pd(ld1), pd(ld1))
      val (gbk1, gbk2) = (gbk(flatten(pd1)), gbk(pd2))
      val fl1 = flatten(gbk1, gbk2)

      (gbk1 -> relatedGbks) ==== IdSet(gbk2)
    }
  }

}
trait nodes extends factory {
  lazy val l1   = load
  lazy val pd1  = pd(l1)
  lazy val gbk1 = gbk(pd1)
  lazy val fl1  = flatten(gbk1, pd1)
  lazy val mat1 = mt(fl1)
}
trait factory extends Scope with CompNodeFactory with MscrAttributes {
  override def load                                   = init(super.load)
  override def flatten[A](nodes: CompNode*)           = init(super.flatten(nodes:_*))
  override def parallelDo(in: CompNode)               = init(super.parallelDo(in))
  override def rt                                     = init(super.rt)
  override def cb(in: CompNode)                       = init(super.cb(in))
  override def gbk(in: CompNode)                      = init(super.gbk(in))
  override def mt(in: CompNode)                       = init(super.mt(in))
  override def op[A, B](in1: CompNode, in2: CompNode) = init(super.op(in1, in2))
  override def pd(in: CompNode, env: CompNode = Return(()), groupBarrier: Boolean = false, fuseBarrier: Boolean = false) =
    init(super.pd(in, env, groupBarrier, fuseBarrier))

  /** show before and after the optimisation */
  def optimisation(node: CompNode, optimised: CompNode) =
    if (show(node) != show(optimised)) "INITIAL: \n"+show(node)+"\nOPTIMISED:\n"+show(optimised) else "no optimisation"

  def show(node: CompNode): String =
    "SHOWING NODE: "+showNode(node, None)+"\n"+mscrsGraph(init(ancestors(node).headOption.getOrElse(node)))
}
trait graph extends MscrGraph

