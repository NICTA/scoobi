/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package acceptance

import impl.plan.comp._
import testing.mutable.NictaSimpleJobs
import impl.ScoobiConfiguration
import core.{ProcessNode, DList}
import impl.plan.DListImpl

class RandomDListsSpecification extends NictaSimpleJobs with CompNodeData {
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
    new DListImpl[String](Optimiser.reinitAttributable(Optimiser.duplicate(list.getComp).asInstanceOf[ProcessNode]))
  }

}


















































































































