package com.nicta.scoobi
package acceptance

import impl.plan.comp._
import testing.mutable.NictaSimpleJobs
import impl.ScoobiConfiguration
import core.DList
import impl.plan.DListImpl

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {
  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l1: DList[String]) =>
    compareExecutions(l1)
  }.set(minTestsOk -> 20)

  def compareExecutions(l1: DList[String]) = {
    val locally  = duplicate(l1).run(configureForLocal(ScoobiConfiguration()))
    val inMemory = duplicate(l1).run(configureForInMemory(ScoobiConfiguration()))

    locally aka "the local hadoop results" must haveTheSameElementsAs(inMemory)

    "====== EXAMPLE OK ======\n".pp; ok
  }

  // this duplicate is to avoid some yet unexplained undue failures when running the tests
  def duplicate(list: DList[String]) = {
    new DListImpl[String](Optimiser.reinitAttributable(Optimiser.duplicate(list.getComp).asInstanceOf[ProcessNode]))(list.wf)
  }

}


















































































































