package com.nicta.scoobi
package application

import testing.mutable.{SimpleJobs, HadoopSpecification}
import Scoobi._
import impl.plan.comp._

class InMemoryModeSpec extends HadoopSpecification with SimpleJobs with CompNodeData { sequential

  "The in memory mode can execute DLists and DObjects with repeating shared computations".txt

  "Basic computations for DLists" >> {
    "ParallelDo" >> { implicit sc: ScoobiConfiguration =>
      DList(1, 2, 3).map(_ + 1).run === Seq(2, 3, 4)
    }
    "Combine" >> { implicit sc: ScoobiConfiguration =>
      DList((1, Seq(2, 3)), (3, Seq(4))).combine((_:Int) + (_:Int)).run === Seq((1, 5), (3, 4))
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
