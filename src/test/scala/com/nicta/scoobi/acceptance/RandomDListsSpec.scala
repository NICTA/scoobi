package com.nicta.scoobi
package acceptance

import impl.plan.comp.{StringSink, CompNodeData}
import testing.NictaSimpleJobs
import application.{DLists, ScoobiConfiguration}
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
    val rt1 = rt
    val l1  = new DListImpl(pd(pd(flatten(load, load), rt1), rt1))

    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))
    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))

    locally must haveTheSameElementsAs(inMemory)
  }
}


















































































































