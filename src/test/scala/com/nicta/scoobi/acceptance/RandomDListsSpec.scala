package com.nicta.scoobi
package acceptance

import impl.plan.comp._
import testing.mutable.NictaSimpleJobs
import application.ScoobiConfiguration
import core.DList

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {
  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l1: DList[String]) =>
    compareExecutions(l1)
  }.set(minTestsOk -> 20)

  def compareExecutions(l1: DList[String]) = {
    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))
    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))

    locally must haveTheSameElementsAs(inMemory)

    "====== EXAMPLE OK ======\n".pp; ok
  }


}


















































































































