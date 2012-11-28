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
import com.nicta.scoobi.Scoobi._

class RandomDListsSpec extends NictaSimpleJobs with CompNodeData {

  "A DList must return an equivalent result, whether it's executed in memory or locally" >> prop { (l1: DList[String]) =>
    compareExecutions(l1)
  }.set(minTestsOk -> 20)

  def compareExecutions(l1: DList[String]) = {
    val inMemory = l1.run(configureForInMemory(ScoobiConfiguration()))
    val locally  = l1.run(configureForLocal(ScoobiConfiguration()))

    locally must haveTheSameElementsAs(inMemory)

    "====== EXAMPLE OK ======\n".pp; ok
  }


}


















































































































