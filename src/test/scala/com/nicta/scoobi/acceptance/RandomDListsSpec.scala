package com.nicta.scoobi
package acceptance

import impl.plan.comp.CompNodeData
import testing.NictaSimpleJobs
import core.DList
import application.ScoobiConfiguration
import impl.plan.DListImpl

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {

  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l1: DList[String]) =>
    val list =     new DListImpl(mt(pd(gbk(cb(gbk(pd(load)))))))

    val inMemory = list.run(configureForInMemory(ScoobiConfiguration()))
    val locally  = list.run(configureForLocal(ScoobiConfiguration()))
    inMemory === locally
  }

}
