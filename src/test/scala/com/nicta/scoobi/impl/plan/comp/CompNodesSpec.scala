package com.nicta.scoobi
package impl
package plan
package comp

import testing.mutable.UnitSpecification
import org.specs2.specification.AllExpectations

class CompNodesSpec extends UnitSpecification with AllExpectations {

  "the inputs of a node are its children" >> new nodes {
    val load0 = load
    val cb1 = cb(load0)
    (cb1 -> inputs) ==== Seq(load0)
  }
  "the inputs of a parallelDo only contain the input node, not the environment" >> new nodes {
    val ld1 = load
    (pd(ld1) -> inputs) ==== Seq(ld1)
  }
  "the ancestors of a node are all its direct parents" >> new nodes {
    (pd1 -> ancestors) ==== Seq(fl1, mat1)
    (l1 -> ancestors)  ==== Seq(pd1, fl1, mat1)
  }
  "a node cannot be a strict parent of itself" >> new nodes {
    (fl1 -> isStrictParentOf(fl1)) === false
  }
  "two nodes related by a common input are not strict parents" >> new factory {
    val ld1 = load
    val (pd1, pd2) = (pd(ld1), pd(ld1))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    (gbk1 -> isStrictParentOf(gbk2)) === false
    (gbk2 -> isStrictParentOf(gbk1)) === false
  }
  "the parents of a node are all the nodes having this node in their descendents" >> new nodes {
    (fl1 -> parents) ==== Set(mat1)
    (pd1 -> parents) ==== Set(fl1, gbk1, mat1)
    (l1 -> parents)  ==== Set(pd1, fl1, gbk1, mat1)
  }
  "the descendents of a node is the recursive list of all children" >> new nodes {
    (mat1 -> descendents) ==== Seq(fl1, gbk1, pd1, l1, pd1.env)
  }
  "the descendents must be collected along all paths of the graph" >> new factory {
    val ld1 = load
    val (pd1, pd2) = (pd(ld1), pd(ld1))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    val fl1 = flatten(gbk1, gbk2)

    (gbk1 -> descendents) ==== Seq(pd1, ld1, pd1.env)
    (fl1-> descendents)   ==== Seq(gbk1, gbk2, pd1, ld1, pd1.env, pd2, pd2.env)
  }
  "the outputs of a node are all its direct parents" >> new nodes {
    (pd1 -> outputs) ==== Seq(fl1, gbk1)
  }
  endp

  "2 nodes are parentOf if" >> {
    "one is the parent of the other through the 'parents' relationship" >> new factory {
      val ld1 = load
      val pd1 = pd(ld1)
      (pd1 -> isParentOf(ld1)) === true
      (ld1 -> isParentOf(pd1)) === true
    }
    "one is equal to the other" >> new factory {
      val pd1 = pd(load)
      (pd1 -> isParentOf(pd1)) === true
    }
  }
  "it is possible to get all the nodes which use a given node as an environment" >> new factory {
    val mt1 = mt(load)
    val pd1 = pd(load, mt1)

    (mt1 -> usesAsEnvironment) === Seq(pd1)
  }
}

trait nodes extends factory {
  /**
   *       ld
   *       /
   *      pd1
   *      /  \
   *    gbk1  \
   *      \    \
   *      flatten1
   *        |
   *       mat1
   */
  lazy val l1   = load
  lazy val pd1  = pd(l1)
  lazy val gbk1 = gbk(pd1)
  lazy val fl1  = flatten(gbk1, pd1)
  lazy val mat1 = {
    val root = mt(fl1)
    initTree(root)
    root
  }
}