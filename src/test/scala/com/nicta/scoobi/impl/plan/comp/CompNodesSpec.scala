package com.nicta.scoobi
package impl
package plan
package comp

import testing.mutable.UnitSpecification
import org.specs2.specification.AllExpectations
import core.CompNode

class CompNodesSpec extends UnitSpecification with AllExpectations with CompNodeData {

  "CompNodes equality is based on a unique node id" >> prop { (node1: CompNode, node2: CompNode) =>
    node1 must be_==(node2).iff(node1.id == node2.id)
  }

  "CompNodes hashCode is based on a unique node id" >> prop { (node1: CompNode, node2: CompNode) =>
    node1.hashCode must be_==(node2.hashCode).when(node1.id == node2.id)
  }

  "the inputs of a node are its children" >> new nodes {
    val load0 = load
    val cb1 = cb(load0)
    (cb1 -> inputs) ==== Seq(load0)
  }
  "the inputs of a parallelDo only contain the input node, not the environment" >> new nodes {
    val ld1 = load
    (pd(ld1) -> inputs) ==== Seq(ld1)
  }

  "the outputs of a node are all the nodes using it, but not using it as an environment" >> new nodes {
    (pd1 -> outputs) ==== Seq(gbk1, pds1)
  }
  endp

  "it is possible to get all the nodes which use a given node as an environment" >> new factory {
    val mt1 = mt(load)
    val pd1 = pd(load, mt1)

    (mt1 -> usesAsEnvironment) === Seq(pd1)
  }
}

trait nodes extends factory {
  lazy val l1   = load
  lazy val pd1  = pd(l1)
  lazy val gbk1 = gbk(pd1)
  lazy val pds1 = pd(gbk1, pd1)
  lazy val mat1 = mt(pds1)
}