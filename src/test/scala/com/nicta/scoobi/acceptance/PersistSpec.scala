package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import core.ScoobiConfiguration
import Scoobi._
import testing.TempFiles
import collection.mutable.ListBuffer

class PersistSpec extends NictaSimpleJobs {

  "There are many ways to execute computations with DLists or DObjects".txt

  "a single DList, simply add a sink, like a TextFile and persist the list" >> { implicit sc: ScoobiConfiguration =>
    val resultFile = TempFiles.createTempFile("test")
    val list = DList(1, 2, 3).toTextFile(resultFile.getPath)

    list.persist
    resultFile must exist
  }
  "a single DObject" >> { implicit sc: ScoobiConfiguration =>
    val o1 = DList(1, 2, 3).sum
    o1.run === 6
  }
  "a list, materialized" >> { implicit sc: ScoobiConfiguration =>
    val list = DList(1, 2, 3)
    list.run === Seq(1, 2, 3)
  }
  "a list, having a sink and also materialized" >> { implicit sc: ScoobiConfiguration =>
    val resultFile = TempFiles.createTempFile("test")
    val list = DList(1, 2, 3).toTextFile(resultFile.getPath)

    list.run === Seq(1, 2, 3)
    resultFile must exist
  }
  "2 materialized lists with a common ancestor" >> {
    "when we only want one list, only the computations for that list must be done" >> { implicit sc: ScoobiConfiguration =>
      val computations = new ListBuffer[String]
      def computing(s: String) { computations.append(s) }

      val l1 = DList(1, 2, 3).map(i => { computing("l1"); i * 10 })
      val l2 = l1.map(i => { computing("l2"); i + 1 })
      val l3 = l1.map(i => { computing("l3"); i + 2 })

      l2.run   === Seq(11, 21, 31)
      "the list l1 must have been computed only once and l3 must not have been computed" ==> {
        computations must contain("l1", "l2")
        computations must not contain("l3")
      }
    }

    "when we want both lists, the shared computation must be computed only once" >> { implicit sc: ScoobiConfiguration =>
      val computations = new ListBuffer[String]
      def computing(s: String) { computations.append(s) }

      val l1 = DList(1, 2, 3).map(i => { computing("l1"); i * 10 })
      val l2 = l1.map(i => { computing("l2"); i + 1 })
      val l3 = l1.map(i => { computing("l3"); i + 2 })

      persist(l2, l3)
      (l2.run, l3.run) === (Seq(11, 21, 31), Seq(12, 22, 32))

      "the list l1 must have been computed only once" ==> {
        computations.filter(_ == "l1") must have size(3)
      }
    }
  }
}
