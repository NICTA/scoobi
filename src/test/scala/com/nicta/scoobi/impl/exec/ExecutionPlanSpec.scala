package com.nicta.scoobi
package impl
package exec

import org.kiama.rewriting.Rewriter
import Rewriter._
import core._
import plan._
import mscr._
import comp._
import exec._
import mapreducer.Env
import application.ScoobiConfiguration
import testing.mutable.UnitSpecification

class ExecutionPlanSpec extends UnitSpecification { sequential

  "The execution execPlan transforms Mscrs and nodes into a graph of executable Mscrs and executable nodes".txt

  "Mscrs are transformed into mscrs with actual channels" >> {
    "each Mscr must transform its channels to become an executable Mscr" >> new plans {
      transform(Mscr(MapperInputChannel(), GbkOutputChannel(gbkLoad))) ===
        MscrExec(Set(MapperInputChannelExec(Seq())), Set(GbkOutputChannelExec(gbkExec)))
    }
    "input channels must be transformed to executable input channels, containing executable nodes" >> {
      "A MapperInputChannel is transformed to a MapperInputChannelExec" >> new plans {
        transform(MapperInputChannel(pdLoad)) === MapperInputChannelExec(Seq(MapperExec(Ref(pdLoad), loadExec)))
      }
      "An IdInputChannel is transformed into a BypassInputChannelExec" >> new plans {
        transform(IdInputChannel(ld, gbkLoad)) === BypassInputChannelExec(loadExec, gbkExec)
      }
      "A StraightInputChannel is transformed into a StraightInputChannelExec" >> new plans {
        transform(StraightInputChannel(ld)) === StraightInputChannelExec(loadExec)
      }
    }
    "output channels must be transformed to executable output channels, containing executable nodes" >> {
      "A GbkOutputChannel is transformed to a GbkOutputChannelExec" >> {
        "with a gbk node only" >> new plans {
          transform(GbkOutputChannel(gbkLoad)) === GbkOutputChannelExec(gbkExec)
        }
        "with optional nodes" >> new plans {
          transform(GbkOutputChannel(gbkLoad, Some(flattenLoad))) ===
            GbkOutputChannelExec(gbkExec, Some(flattenExec))
        }
      }
      "An FlattenOutputChannel is transformed into a FlattenOutputChannelExec" >> new plans {
        transform(FlattenOutputChannel(flattenLoad)) === FlattenOutputChannelExec(flattenExec)
      }
      "A BypassOutputChannel is transformed into a BypassOutputChannelExec" >> new plans {
        transform(BypassOutputChannel(pdLoad)) === BypassOutputChannelExec(pdExec)
      }
    }
    "output channels must be tagged with an Int index starting from 0" >> new plans {
      val mscrExec = transform(new Mscr(Set(MapperInputChannel()), Set(GbkOutputChannel(gbkLoad): OutputChannel,
                                                                       FlattenOutputChannel(flattenLoad),
                                                                       BypassOutputChannel(pdLoad))))
      mscrExec.outputChannels.map(tag) === Set(0, 1, 2)
    }
    "inputs must be tagged with a set of Ints relating the input nodes to the tag of the correspdonding output channel" >> new plans {
      val mscrExec = transform(new Mscr(Set(MapperInputChannel(pdLoad, pdLoad)),
                                        Set(GbkOutputChannel(gbk(pdLoad)): OutputChannel,
                                            FlattenOutputChannel(flatten(pdLoad)))))

      mscrExec.inputChannels.map(tags(mscrExec)) === Set(Map((pdLoad, Set(0, 1))))
    }
  }

  "Graph nodes are transformed to more specific nodes" >> {
    "Load nodes remain unchanged" >> new plans {
      val l = load
      execPlan(l) === Seq(LoadExec(Ref(l)))
    }

    "Return nodes remain unchanged" >> new plans {
      val ret = rt
      execPlan(ret) === Seq(ReturnExec(Ref(ret)))
    }

    "Materialize nodes remain unchanged" >> new plans {
      val mat = mt(load)
      execPlan(mat) === Seq(MaterializeExec(Ref(mat), loadExec))

      val mat1 = mt(mat)
      execPlan(mat1) === Seq(MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), loadExec)))

      val mat2 = mt(mat1)
      execPlan(mat2) === Seq(MaterializeExec(Ref(mat2), MaterializeExec(Ref(mat1), MaterializeExec(Ref(mat), loadExec))))
    }

    "Op nodes remain unchanged" >> new plans {
      val add = op(load, load)
      execPlan(add) === Seq(OpExec(Ref(add), loadExec, loadExec))
    }

    "Combine nodes remain unchanged" >> new plans {
      val cb1 = cb(load)
      execPlan(cb1) === Seq(CombineExec(Ref(cb1), loadExec))
    }

    "GroupByKey nodes remain unchanged" >> new plans {
      val gbk1 = gbk(load)
      execPlan(gbk1) === Seq(GroupByKeyExec(Ref(gbk1), loadExec))
    }

    "Flatten nodes remain unchanged" >> new plans {
      val (l, ret) = (load, rt)
      val fl = flatten[String](l, ret)
      execPlan(fl) === Seq(FlattenExec(Ref(fl), Vector[ExecutionNode](LoadExec(Ref(l)), ReturnExec(Ref(ret)))))
    }

    "Parallel do nodes are transformed into Mappers or Reducers" >> {
      "Mapper if input is: Load, ParallelDo, Flatten" >> {
        "input is: Load" >> new plans {
          val pd1 = pd(load)
          execPlan(pd1) === Seq(MapperExec(Ref(pd1), loadExec))
        }
        "input is: ParallelDo" >> new plans {
          val (pd1, pd2) = (pd(load), pd(pd(load)))
          execPlan(pd2) === Seq(MapperExec(Ref(pd2), MapperExec(Ref(pd1), loadExec)))
        }
        "input is: Flatten" >> new plans {
          val pd1 =  pd(flatten[String](load))
          execPlan(pd1) === Seq(MapperExec(Ref(pd1), FlattenExec(Ref(flatten[String](load)), List(loadExec))))
        }
      }
      "Mapper if input is: Gbk, Combine and the node is a mapper" >> new plans {
        val (gbk1, cb1) = (gbk(load), cb(load))
        val (pd1, pd2) = (pd(gbk1), pd(cb1))
        val fl = flatten(pd1, pd2)
        (pd1 -> isMapper) aka "parallel do must be a mapper" must beTrue // because pd1 has outputs

        execPlan(pd1, pd2) ===
          Seq(MapperExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), loadExec)),
              MapperExec(Ref(pd2), CombineExec(Ref(cb1), loadExec)))
      }
      "Reducer if input is: Gbk, Combine and the node is a reducer in the Mscr" >>  new plans {
        val (gbk1, cb1) = (gbk(load), cb(load))
        val (pd1, pd2) = (pd(gbk1, groupBarrier = true), pd(cb1, groupBarrier = true))
        (pd1 -> isReducer) must beTrue // because pd1 has a barrier

        execPlan(pd1, pd2) ===
          Seq(GbkReducerExec(Ref(pd1), GroupByKeyExec(Ref(gbk1), loadExec)),
            ReducerExec(Ref(pd2), CombineExec(Ref(cb1), loadExec)))
      }
      "GbkMapper if output is a GroupByKey" >> new plans {
        val pd1 = pd(load)
        val gbk1 = gbk(pd1)
        execPlan(gbk1) === Seq(GroupByKeyExec(Ref(gbk1), GbkMapperExec(Ref(pd1), Ref(gbk1), loadExec)))
      }
      "error if input is: Materialize, Op or Return" >> new plans {
        execPlan(pd(mt(load)))       must throwAn[Exception]
        execPlan(pd(op(load, load))) must throwAn[Exception]
        execPlan(pd(rt))             must throwAn[Exception]
      }
    }
  }
}
trait plans extends execfactory with ExecutionPlan {

  def execPlan(nodes: CompNode*) =
    createExecutionGraph(Vector(nodes:_*))

  def execPlanForInputChannels(inputChannels: InputChannel*) =
    createExecutionPlanInputChannels(inputChannels)

  def transform(mscr: Mscr): MscrExec =
    rewrite(rewriteMscr)(mscr).asInstanceOf[MscrExec]

  def transform(channel: Channel) =
    rewrite(rewriteChannels)(channel)

}

trait execfactory extends factory {
  lazy val ld                = load
  lazy val pdLoad            = pd(load)
  lazy val flattenLoad       = flatten[String](ld)
  lazy val flattenPdLoad     = flatten[String](pdLoad)
  lazy val gbkLoad           = gbk(ld)
  lazy val cbLoad            = cb(ld)
  lazy val cbLoadExec        = CombineExec(Ref(cbLoad), load)
  lazy val loadExec          = LoadExec(Ref(ld))
  lazy val gbkExec           = GroupByKeyExec(Ref(gbkLoad), loadExec)
  lazy val gbkCombinerExec   = CombineExec(Ref(cbLoad), cbLoadExec)
  lazy val gbkReducerExec    = GbkReducerExec(Ref(pdLoad), cbLoadExec)
  lazy val flattenExec       = FlattenExec(Ref(flattenLoad), Seq(loadExec))
  lazy val flattenPdExec     = FlattenExec(Ref(flattenPdLoad), Seq(loadExec))
  lazy val pdExec            = MapperExec(Ref(pdLoad), loadExec)
}
