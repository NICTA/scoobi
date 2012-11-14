package com.nicta.scoobi
package acceptance

import impl.plan.comp.CompNodeData
import testing.NictaSimpleJobs
import core.DList
import application.ScoobiConfiguration
import impl.plan.DListImpl

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {

  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l2: DList[String]) =>
    val rt1 = rt
    val l1 =     new DListImpl(pd(pd(pd(flatten(load, load), rt1), rt1), rt1))

    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))
    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))
    inMemory === locally
  }

}
