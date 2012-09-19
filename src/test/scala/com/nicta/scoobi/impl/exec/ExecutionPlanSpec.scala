package com.nicta.scoobi
package impl
package exec

import org.specs2.mutable.Specification
import ExecutionPlan._
import plan._
import graph._
import comp.{CompNode, CompNodeFactory}
import application.ScoobiConfiguration
import core.WireFormat
import graph.{GbkOutputChannel, InputChannel, MapperInputChannel, StraightInputChannel, BypassOutputChannel, FlattenOutputChannel}
import org.kiama.rewriting.Rewriter

class ExecutionPlanSpec extends Specification with Plans {
  "The execution execPlan transforms Mscrs and nodes into a graph of executable Mscrs and executable nodes".txt

  "Mscrs are transformed into mscrs with actual channels" >> {
    "each Mscr must transform its channels to become an executable Mscr" >> {
      transform(Mscr(Set(MapperInputChannel(Set())), Set(GbkOutputChannel(gbk(load))))) ===
        MscrExec(Set(MapperInputChannelExec(Seq())), Set(GbkOutputChannelExec(gbkExec)))
    }
    "input channels must be transformed to executable input channels, containing executable nodes" >> {
      "A MapperInputChannel is transformed to a MapperInputChannelExec" >> {
        transform(MapperInputChannel(Set(pd(load)))) === MapperInputChannelExec(Seq(MapperExec(Ref(pd(load)), loadExec)))
      }
      "An IdInputChannel is transformed into a BypassInputChannelExec" >> {
        transform(IdInputChannel(load)) === BypassInputChannelExec(loadExec)
      }
      "A StraightInputChannel is transformed into a StraightInputChannelExec" >> {
        transform(StraightInputChannel(load)) === StraightInputChannelExec(loadExec)
      }
    }
    "output channels must be transformed to executable output channels, containing executable nodes" >> {
      "A GbkOutputChannel is transformed to a GbkOutputChannelExec" >> {
        "with a gbk node only" >> {
          transform(GbkOutputChannel(gbk(load))) === GbkOutputChannelExec(gbkExec)
        }
        "with optional nodes" >> {
          transform(GbkOutputChannel(gbk(load), Some(flatten[String](load)))) ===
            GbkOutputChannelExec(gbkExec, Some(flattenExec))
        }
      }
      "An FlattenOutputChannel is transformed into a FlattenOutputChannelExec" >> {
        transform(FlattenOutputChannel(flatten(load))) === FlattenOutputChannelExec(flattenExec)
      }
      "A BypassOutputChannel is transformed into a BypassOutputChannelExec" >> {
        transform(BypassOutputChannel(pd(load))) === BypassOutputChannelExec(pdExec)
      }
    }
  }

  "Graph nodes are transformed to more specific nodes" >> {
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
      execPlan(mat) === Seq(MaterializeExec(Ref(mat), loadExec))

      val mat1 = mt(mat)
      execPlan(mat1) === Seq(MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), loadExec)))

      val mat2 = mt(mat1)
      execPlan(mat2) === Seq(MaterializeExec(Ref(mat2), MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), loadExec))))
    }

    "Op nodes remain unchanged" >> {
      val add = op[String, String](load, load)
      execPlan(add) === Seq(OpExec(Ref(add), loadExec, loadExec))
    }

    "Combine nodes remain unchanged" >> {
      val cb1 = cb(load)
      execPlan(cb1) === Seq(CombineExec(Ref(cb1), loadExec))
    }

    "GroupByKey nodes remain unchanged" >> {
      val gbk1 = gbk(load)
      execPlan(gbk1) === Seq(GroupByKeyExec(Ref(gbk1), loadExec))
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
          Seq(MapperExec(Ref(pd1), loadExec),
            MapperExec(Ref(pd2), MapperExec(Ref(pd1), loadExec)),
            MapperExec(Ref(pd3), FlattenExec(Ref(flatten[String](load)), Vector(loadExec))))
      }
      "Mapper if input is: Gbk, Combine and the node is a mapper" >> new factory {
        val (gbk1, cb1) = (gbk(load), cb(load))
        val (pd1, pd2) = (pd(gbk1), pd(cb1))
        val fl = flatten(pd1, pd2)
        (pd1 -> isMapper) must beTrue // because pd1 has outputs

        execPlan(pd1, pd2) ===
          Seq(MapperExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), loadExec)),
              MapperExec(Ref(pd2), CombineExec(Ref(cb1), loadExec)))
      }
      "Reducer if input is: Gbk, Combine and the node is a reducer in the Mscr" >>  new factory {
        val (gbk1, cb1) = (gbk(load), cb(load))
        val (pd1, pd2) = (pd(gbk1, groupBarrier = true), pd(cb1, groupBarrier = true))
        (pd1 -> isReducer) must beTrue // because pd1 has a barrier

        execPlan(pd1, pd2) ===
          Seq(GbkReducerExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), loadExec)),
            ReducerExec(Ref(pd2), CombineExec(Ref(cb1), loadExec)))
      }
      "error if input is: Materialize, Op or Return" >> {
        execPlan(pd(mt(load)))       must throwAn[Exception]
        execPlan(pd(op(load, load))) must throwAn[Exception]
        execPlan(pd(rt))             must throwAn[Exception]
      }
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

import Rewriter._
trait Plans extends CompNodeFactory {
  def execPlan(nodes: CompNode*) =
    createExecutionGraph(Vector(nodes:_*))

  def execPlanForMscrs(mscrs: Mscr*) =
    createExecutionPlan(mscrs)

  def execPlanForInputChannels(inputChannels: InputChannel*) =
    createExecutionPlanInputChannels(inputChannels)

  def environments(nodes: CompNode*)(implicit sc: ScoobiConfiguration) =
    collectEnvironments(Vector(nodes:_*))

  def transform(mscr: Mscr): Any =
    rewrite(rewriteMscr)(mscr)

  def transform(channel: Channel) =
    rewrite(rewriteChannels)(channel)

  lazy val loadExec    = LoadExec(Ref(load))
  lazy val gbkExec     = GroupByKeyExec(Ref(gbk(load)), loadExec)
  lazy val flattenExec = FlattenExec(Ref(flatten[String](load)), Seq(loadExec))
  lazy val pdExec      = MapperExec(Ref(pd(load)), loadExec)
}
