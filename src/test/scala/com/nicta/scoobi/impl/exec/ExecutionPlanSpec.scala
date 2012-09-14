package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import ExecutionPlan._
import plan._
import comp.{CompNode, CompNodeFactory}
import application.ScoobiConfiguration
import core.WireFormat

class ExecutionPlanSpec extends Specification with CompNodeFactory with Plans {
  "The execution execPlan transforms CompNodes into Execution nodes, depending where the CompNodes are in the Mscr graph".txt

  "Load nodes remain unchanged" >> {
    val l = load
    execPlan(l) === Seq(LoadExec(Ref(l)))
  }

  "Return nodes remain unchanged" >> {
    val ret = rt
    execPlan(ret) === Seq(ReturnExec(Ref(ret)))
  }

  "Materialize nodes remain unchanged" >> {
    val mat = mt(load)
    execPlan(mat) === Seq(MaterializeExec(Ref(mat), LoadExec(Ref(load))))

    val mat1 = mt(mat)
    execPlan(mat1) === Seq(MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), LoadExec(Ref(load)))))

    val mat2 = mt(mat1)
    execPlan(mat2) === Seq(MaterializeExec(Ref(mat2), MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), LoadExec(Ref(load))))))
  }

  "Flatten nodes remain unchanged" >> {
    val (l, ret) = (load, rt)
    val fl = flatten[String](l, ret)
    execPlan(fl) === Seq(FlattenExec(Ref(fl), Vector(LoadExec(Ref(l)), ReturnExec(Ref(ret)))))
  }

  "Environments".newp

  "Environments can be build from" >> {
    "ReturnExec nodes" >> {
      val ret = rt
      val envs = environments(ret)(ScoobiConfiguration())
      envs must have size(1)
      envs.head.wf.getClass.getName must_== implicitly[WireFormat[String]].getClass.getName
    }
  }
}

trait Plans {
  def execPlan(nodes: CompNode*) =
    createExecutionPlan(Vector(nodes:_*), Set())

  def environments(nodes: CompNode*)(implicit sc: ScoobiConfiguration) =
    collectEnvironments(Vector(nodes:_*), Set())
}
