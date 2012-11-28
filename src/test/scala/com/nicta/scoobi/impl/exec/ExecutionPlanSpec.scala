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

class ExecutionPlanSpec extends UnitSpecification {
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
