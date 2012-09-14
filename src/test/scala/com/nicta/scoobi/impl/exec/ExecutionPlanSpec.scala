package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import ExecutionPlan._
import plan._
import comp.{CompNode, CompNodeFactory}
import application.ScoobiConfiguration
import core.WireFormat
import graph.factory

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

  "Op nodes remain unchanged" >> {
    val add = op[String, String](load, load)
    execPlan(add) === Seq(OpExec(Ref(add), LoadExec(Ref(load)), LoadExec(Ref(load))))
  }

  "Combine nodes remain unchanged" >> {
    val cb1 = cb(load)
    execPlan(cb1) === Seq(CombineExec(Ref(cb1), LoadExec(Ref(load))))
  }

  "GroupByKey nodes remain unchanged" >> {
    val gbk1 = gbk(load)
    execPlan(gbk1) === Seq(GroupByKeyExec(Ref(gbk1), LoadExec(Ref(load))))
  }

  "Flatten nodes remain unchanged" >> {
    val (l, ret) = (load, rt)
    val fl = flatten[String](l, ret)
    execPlan(fl) === Seq(FlattenExec(Ref(fl), Vector(LoadExec(Ref(l)), ReturnExec(Ref(ret)))))
  }

  "Parallel do nodes are transformed into Mappers or Reducers" >> {
    "Mapper if input is: Load, ParallelDo, Flatten" >> {
      val (pd1, pd2, pd3) = (pd(load), pd(pd(load)), pd(flatten[String](load)))
      execPlan(pd1, pd2, pd3) ===
        Seq(MapperExec(Ref(pd1), LoadExec(Ref(load))),
            MapperExec(Ref(pd2), MapperExec(Ref(pd1), LoadExec(Ref(load)))),
            MapperExec(Ref(pd3), FlattenExec(Ref(flatten[String](load)), Vector(LoadExec(Ref(load))))))
    }
    "Mapper if input is: Gbk, Combine and the node is a mapper" >> new factory {
      val (gbk1, cb1) = (gbk(load), cb(load))
      val (pd1, pd2) = (pd(gbk1), pd(cb1))
      val fl = flatten(pd1, pd2)
      (pd1 -> isMapper) must beTrue // because pd1 has outputs

      execPlan(pd1, pd2) ===
        Seq(MapperExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), LoadExec(Ref(load)))),
            MapperExec(Ref(pd2), CombineExec(Ref(cb1), LoadExec(Ref(load)))))
    }
    "Reducer if input is: Gbk, Combine and the node is a reducer in the Mscr" >>  new factory {
      val (gbk1, cb1) = (gbk(load), cb(load))
      val (pd1, pd2) = (pd(gbk1, groupBarrier = true), pd(cb1, groupBarrier = true))
      (pd1 -> isReducer) must beTrue // because pd1 has a barrier

      execPlan(pd1, pd2) ===
        Seq(GbkReducerExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), LoadExec(Ref(load)))),
            ReducerExec(Ref(pd2), CombineExec(Ref(cb1), LoadExec(Ref(load)))))
    }
    "error if input is: Materialize, Op or Return" >> {
      execPlan(pd(mt(load)))       must throwAn[Exception]
      execPlan(pd(op(load, load))) must throwAn[Exception]
      execPlan(pd(rt))             must throwAn[Exception]
    }
  }

  "Environments".newp

  "Environments can be build from" >> {
    def envsMustHaveWireFormat(envs: Seq[Env[_]]) =
      envs.head.wf.getClass.getName must_== implicitly[WireFormat[String]].getClass.getName

    "ReturnExec nodes" >> {
      val envs = environments(rt, rt)(ScoobiConfiguration())
      envs must have size(2)
      envsMustHaveWireFormat(envs)
    }
    "MaterializeExec nodes" >> {
      val envs = environments(mt(load), mt(load))(ScoobiConfiguration())
      envs must have size(2)
      envsMustHaveWireFormat(envs)
    }
    "OpExec nodes" >> {
      val envs = environments(op(load, load))(ScoobiConfiguration())
      envs must have size(1)
      envsMustHaveWireFormat(envs)
    }
  }
}

trait Plans {
  def execPlan(nodes: CompNode*) =
    createExecutionPlan(Vector(nodes:_*), Set())

  def environments(nodes: CompNode*)(implicit sc: ScoobiConfiguration) =
    collectEnvironments(Vector(nodes:_*), Set())
}
