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
package application

import testing.SimpleJobs
import testing.mutable.HadoopSpecification
import Scoobi._
import impl.plan.comp._
import core.Reduction._

class InMemoryModeSpec extends HadoopSpecification with SimpleJobs with CompNodeData { sequential

  "The in memory mode can execute DLists and DObjects with repeating shared computations".txt

  "Basic computations for DLists" >> {
    "ParallelDo" >> { implicit sc: ScoobiConfiguration =>
      DList(1, 2, 3).map(_ + 1).run === Seq(2, 3, 4)
    }
    "Combine" >> { implicit sc: ScoobiConfiguration =>
      DList((1, Seq(2, 3)), (3, Seq(4))).combine(Sum.int).run === Seq((1, 5), (3, 4))
    }
  }

  "Random tests" >> {
    implicit val inMemoryConfiguration = configureForInMemory(configuration)
    "Computing a DList must never fail" >> prop { (list: DList[String]) =>
      list.run must not(throwAn[Exception])
    }
    "Computing a DObject must never fail" >> prop { (o: DObject[String]) =>
      o.run must not(throwAn[Exception])
    }
  }
  override def contexts = Seq(inMemory)
}
