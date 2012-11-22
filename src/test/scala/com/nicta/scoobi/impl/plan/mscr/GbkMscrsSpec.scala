package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import mscr.Mscr._
import comp._
import collection.IdSet
import application.{ScoobiConfiguration, DList}
import testing.NictaSimpleJobs

class GbkMscrsSpec extends MscrMakerSpecification {

  "We must build MSCRs around related GroupByKey nodes".newp

  "if two GroupByKey nodes share the same input, they belong to the same Mscr" >> new factory {
    val load0 = load
    val gbk1  = gbk(pd(load0))
    val gbk2  = gbk(pd(load0))
    val graph = flatten(gbk1, gbk2)
    val mscrs = makeMscrs(graph).filter(isGbkMscr)

    mscrs must have size(1)
    mscrs.toSeq(0).groupByKeys ==== Set(gbk1, gbk2)
  }
  "if two GroupByKey nodes don't share the same input, they belong to 2 different Mscrs" >> new factory {
    val gbk1  = gbk(pd(load))
    val gbk2  = gbk(pd(load))

    makeMscr(gbk1) must not be_==(makeMscr(gbk2))
  }
  "two successive gbks must be in 2 different mscrs" >> new factory {
    val pd0   = load
    val gbk1  = gbk(pd(pd0))
    val pd1   = pd(gbk1)
    val gbk2  = gbk(pd1)
    val graph = flatten(gbk2)
    val mscrs = makeMscrs(graph)

    mscrs.map(_.groupByKeys).filterNot(_.isEmpty) ==== Set(IdSet(gbk1), IdSet(gbk2))
  }
  "a Gbk must have the same Mscr that references it" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
    mscrsFor(graph).flatMap(_.groupByKeys) foreach { gbk =>
      val m = (gbk -> mscr)
      m.groupByKeys.map(_.id) aka show(gbk) must contain(gbk.id)
    }
  }
  "a Gbk cannot be included in its siblings" >> prop { (gbk: GroupByKey[_,_], ma: MscrAttributes) => import ma._
    (gbk -> siblings) must not(contain(gbk))
  }
  "the siblings of Gbk cannot contain one of its descendents" >> prop { (gbk: GroupByKey[_,_], ma: MscrAttributes) => import ma._
    (gbk -> siblings) must not(containAnyOf((gbk -> descendents).toSeq))
  }
  "the siblings of Gbk cannot contain one of its descendents in a transitive way" >> prop { (gbk: GroupByKey[_,_], ma: MscrAttributes) => import ma._
    (gbk -> siblings).flatMap(_ -> siblings).flatMap(_ -> siblings) must not(containAnyOf((gbk -> descendents).toSeq))
  }
  "a ParallelDo must not have the same Mscr as its Materialize environment. See issue #127" in new factory {
    val ld1          = load
    val (pd1, pd2)   = (pd(ld1), pd(ld1))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    val mt1          = mt(gbk1)
    val pd3          = pd(gbk2, env = mt1)
    val graph        = mt(pd3)

    (pd3 -> mscr) aka show(graph) must not be_== (pd3.env -> mscr)
  }

  "example of parallel dos sharing the same input" >> new factory {
    val op0          = op(load, load)
    val (pd1, pd2)   = (pd(op0), pd(op0))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    val graph        = flatten(gbk1, gbk2)

    makeMscr(gbk1) === makeMscr(gbk2)
    makeMscr(gbk1) ==== Mscr(inputChannels  = Set(MapperInputChannel(pd2, pd1)),
                             outputChannels = Set(GbkOutputChannel(gbk2), GbkOutputChannel(gbk1)))
  }
  "a ParallelDo can not be a mapper and a reducer at the same time" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
    val m = makeMscr(graph)
    (m.mappers intersect m.reducers) aka show(graph) must beEmpty
  }
  "all the ParallelDos must be in a mapper or a reducer (or an id channel)" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
    val ms = makeMscrs(graph)
    parallelDos(graph) foreach  { p =>
      ms.exists {  m =>
        m.groupByKeys.isEmpty         ||
        m.inputChannels.isEmpty       ||
        m.mappers.contains(p)         ||
        m.idMappers.toSeq.contains(p) ||
        m.reducers.contains(p)
      } must beTrue.unless(ms.isEmpty)
      //aka "for\n"+showGraph(graph, mscr)+"\nMSCRs\n"+ms.mkString("\n") must beTrue.unless(ms.isEmpty)
    }
  }
  "a mapper parallelDo must be in a MapperInputChannel, a reducer parallelDo must be in a BypassOutputChannel" >> new factory {
    val pd1  = pd(load)
    val gbk1 = gbk(pd1)
    val pd2  = pd(gbk1)

    (pd1 -> isMapper) must beTrue
    (pd2 -> isReducer) must beTrue

    makeMscr(gbk1).mappers  aka mscrGraph(gbk1) must_== Set(pd1)
    makeMscr(gbk1).reducers aka mscrGraph(gbk1) must_== Set(pd2)

  }
}

