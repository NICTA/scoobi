package com.nicta.scoobi
package impl
package exec

import plan._
import graph._
import comp.{CompNode, CompNodeFactory}
import application.ScoobiConfiguration
import core.WireFormat
import graph.{GbkOutputChannel, InputChannel, MapperInputChannel, StraightInputChannel, BypassOutputChannel, FlattenOutputChannel}
import org.kiama.rewriting.Rewriter
import org.specs2.specification.Scope
import testing.mutable.UnitSpecification

class ExecutionPlanSpec extends UnitSpecification with plans {
  "The execution execPlan transforms Mscrs and nodes into a graph of executable Mscrs and executable nodes".txt

  """
   - set tags on output  channels
   - set set(tags) in input channels
  """.txt

  "Mscrs are transformed into mscrs with actual channels" >> {
    "each Mscr must transform its channels to become an executable Mscr" >> {
      transform(Mscr(Set(MapperInputChannel(Set())), Set(GbkOutputChannel(gbkLoad)))) ===
        MscrExec(Set(MapperInputChannelExec(Seq())), Set(GbkOutputChannelExec(gbkExec)))
    }
    "input channels must be transformed to executable input channels, containing executable nodes" >> {
      "A MapperInputChannel is transformed to a MapperInputChannelExec" >> {
        transform(MapperInputChannel(Set(pdLoad))) === MapperInputChannelExec(Seq(MapperExec(Ref(pdLoad), loadExec)))
      }
      "An IdInputChannel is transformed into a BypassInputChannelExec" >> {
        transform(IdInputChannel(ld)) === BypassInputChannelExec(loadExec)
      }
      "A StraightInputChannel is transformed into a StraightInputChannelExec" >> {
        transform(StraightInputChannel(ld)) === StraightInputChannelExec(loadExec)
      }
    }
    "output channels must be transformed to executable output channels, containing executable nodes" >> {
      "A GbkOutputChannel is transformed to a GbkOutputChannelExec" >> {
        "with a gbk node only" >> {
          transform(GbkOutputChannel(gbkLoad)) === GbkOutputChannelExec(gbkExec)
        }
        "with optional nodes" >> {
          transform(GbkOutputChannel(gbkLoad, Some(flattenLoad))) ===
            GbkOutputChannelExec(gbkExec, Some(flattenExec))
        }
      }
      "An FlattenOutputChannel is transformed into a FlattenOutputChannelExec" >> {
        transform(FlattenOutputChannel(flattenLoad)) === FlattenOutputChannelExec(flattenExec)
      }
      "A BypassOutputChannel is transformed into a BypassOutputChannelExec" >> {
        transform(BypassOutputChannel(pdLoad)) === BypassOutputChannelExec(pdExec)
      }
    }
    "output channels must be tagged with an Int index starting from 0" >> new plans {
      val mscrExec = transform(Mscr(Set(MapperInputChannel(Set())), Set(GbkOutputChannel(gbkLoad): graph.OutputChannel,
                                                                        FlattenOutputChannel(flattenLoad),
                                                                        BypassOutputChannel(pdLoad))))
      mscrExec.outputChannels.map(_.tag) === Set(0, 1, 2)
    }
    "input channels must be tagged with a set of Ints relating the input nodes to the tag of the correspdonding output channel" >> new plans {
      val mscrExec = transform(Mscr(Set(MapperInputChannel(Set())), Set(GbkOutputChannel(gbkLoad): graph.OutputChannel,
        FlattenOutputChannel(flattenLoad),
        BypassOutputChannel(pdLoad))))

      mscrExec.inputChannels.map(_.tags) === Set(0, 1, 2)
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
        "input is: Load" >> {
          val pd1 = pd(load)
          execPlan(pd1) === Seq(MapperExec(Ref(pd1), loadExec))
        }
        "input is: ParallelDo" >> {
          val (pd1, pd2) = (pd(load), pd(pd(load)))
          execPlan(pd2) === Seq(MapperExec(Ref(pd2), MapperExec(Ref(pd1), loadExec)))
        }
        "input is: Flatten" >> {
          val pd1 =  pd(flatten[String](load))
          execPlan(pd1) === Seq(MapperExec(Ref(pd1), FlattenExec(Ref(flatten[String](load)), List(loadExec))))
        }
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
trait plans extends CompNodeExecFactory with ExecutionPlan with Scope {

  def execPlan(nodes: CompNode*) =
    createExecutionGraph(Vector(nodes:_*))

  def execPlanForMscrs(mscrs: Mscr*) =
    createExecutionPlan(mscrs)

  def execPlanForInputChannels(inputChannels: InputChannel*) =
    createExecutionPlanInputChannels(inputChannels)

  def environments(nodes: CompNode*)(implicit sc: ScoobiConfiguration) =
    collectEnvironments(Vector(nodes:_*))

  def transform(mscr: Mscr): MscrExec =
    rewrite(rewriteMscr)(mscr).asInstanceOf[MscrExec]

  def transform(channel: Channel) =
    rewrite(rewriteChannels)(channel)

}

trait CompNodeExecFactory extends CompNodeFactory {
  lazy val ld          = load
  lazy val pdLoad      = pd(load)
  lazy val flattenLoad = flatten[String](ld)
  lazy val gbkLoad     = gbk(ld)
  lazy val loadExec    = LoadExec(Ref(ld))
  lazy val gbkExec     = GroupByKeyExec(Ref(gbkLoad), loadExec)
  lazy val flattenExec = FlattenExec(Ref(flattenLoad), Seq(loadExec))
  lazy val pdExec      = MapperExec(Ref(pdLoad), loadExec)
}
