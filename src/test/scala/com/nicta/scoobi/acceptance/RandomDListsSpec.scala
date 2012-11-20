package com.nicta.scoobi
package acceptance

import impl.plan.comp.{StringSink, CompNodeData}
import testing.NictaSimpleJobs
import application.{DList, DLists, ScoobiConfiguration}
import core.DList
import impl.plan.DListImpl
import org.kiama.rewriting.Rewriter._
import impl.plan.mscr.Mscr
import impl.mapreducer.MscrReducer
import org.specs2.Specification
import org.specs2.matcher.Matcher
import org.specs2.specification.gen.{When, Given}
import org.specs2.specification.Then
import org.kiama.attribution.Attribution
import core.DList

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {

  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l1: DList[String]) =>

    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))
    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))

    locally must haveTheSameElementsAs(inMemory)
    Attribution.resetMemo()

    "====== EXAMPLE OK ======\n".pp; ok
  }

  "A DList must just work locally" >> prop { (l1: DList[String]) =>

    l1.run(configureForLocal(ScoobiConfiguration()))

    "====== EXAMPLE OK ======\n".pp; ok
  }

  "test" >> {

    /**
    ParallelDo (382) env: Return (3){
[error]     in. Flatten (381){
[error]         +ParallelDo (380) env: Return (3){
[error]             in. Load (374)
[error]             env. Return (3)}
[error]         +ParallelDo (379) env: Return (3){
[error]             in. Load (373)
[error]             env. Return (3)}
[error]     }
[error]     env. Return (3)}'     */

//    val rt1 = rt
//    val gbk1 = gbk(flatten(pd(load), pd(load)))
//    val graph  = gbk1

    //val l1  = new DListImpl(graph)
    val l1 = (DList(("a", "b")) ++ DList(("a", "c"))).groupByKey

    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))
    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))

    locally must haveTheSameElementsAs(inMemory)
  }
}


















































































































