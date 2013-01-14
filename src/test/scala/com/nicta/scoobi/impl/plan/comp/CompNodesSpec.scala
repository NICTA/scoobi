package com.nicta.scoobi
package impl
package plan
package comp

import testing.mutable.UnitSpecification
import org.specs2.specification.AllExpectations
import core.{Environment, WireReaderWriter, VectorEmitterWriter, CompNode}
import core.WireFormat._
import org.apache.hadoop.conf._

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
  endp

  "it is possible to get all the nodes which use a given node as an environment" >> new factory {
    val mt1 = mt(pd(load))
    val pd1 = pdWithEnv(load, mt1)

    (mt1 -> usesAsEnvironment) === Seq(pd1)
  }

  "2 ParallelDo nodes can be fused together" >> {
    "simple case: 2 pass-through ParallelDos, the input value must be unchanged" >> new factory {
      val pd1 = ParallelDo.create(loadWith("hello"))(wireFormat[String])
      val pd2 = ParallelDo.create(pd1)(wireFormat[String])
      val fused = ParallelDo.fuse(pd1, pd2)
      val emitter = VectorEmitterWriter()

      fused.map("world", emitter)(configuration)
      emitter.result === Seq("world")
    }
  }

  /** this configuration doesn't store any value in the environment */
  def configuration = new ScoobiConfigurationImpl() {
    override def newEnv(wf: WireReaderWriter) = new Environment {
      def push(any: Any)(implicit conf: Configuration) {}
      def pull(implicit conf: Configuration) = ((),())
    }
  }
}

trait nodes extends factory {
  lazy val l1   = load
  lazy val pd1  = pd(l1)
  lazy val gbk1 = gbk(pd1)
  lazy val pds1 = pd(gbk1, pd1)
  lazy val mat1 = mt(pds1)
}