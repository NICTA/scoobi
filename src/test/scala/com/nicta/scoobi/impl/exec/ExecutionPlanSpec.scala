package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import ExecutionPlan._
import plan._
import comp.CompNodeFactory

class ExecutionPlanSpec extends Specification with CompNodeFactory {
  "The execution plan transforms CompNodes into Execution nodes, depending where the CompNodes are in the Mscr graph".txt

  "Load nodes remain unchanged" >> {
    val l = load
    createExecutionPlan(Vector(l), Set()) === Seq(LoadExec(l))
  }

  "Return nodes remain unchanged" >> {
    val ret = rt
    createExecutionPlan(Vector(ret), Set()) === Seq(ReturnExec(ret))
  }

  "Flatten nodes remain unchanged" >> {
    val (l, ret) = (load, rt)
    val fl = flatten(l, ret)
    createExecutionPlan(Vector(fl), Set()) === Seq(FlattenExec(fl, Vector(LoadExec(l), ReturnExec(ret))))
  }
}
