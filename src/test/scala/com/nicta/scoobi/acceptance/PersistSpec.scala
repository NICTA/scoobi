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
    val list = DList(1, 2, 3).toTextFile(resultFile.getPath, overwrite = true)

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
    val list = DList(1, 2, 3).toTextFile(resultFile.getPath, overwrite = true)

    resultFile must exist
    list.run === Seq(1, 2, 3)
  }
  endp

  "2 materialized lists with a common ancestor" >> {
    "when we only want one list, only the computations for that list must be done" >> { implicit sc: ScoobiConfiguration =>
      val l1 = DList(1, 2, 3).map(_ * 10)
      val l2 = l1.map(_ + 1 )
      val l3 = l1.map(i => { failure("l3 must not be computed"); i + 2 })

      "the list l1 is computed only once and l3 is not computed" ==> {
        l2.run must not(throwAn[Exception])
        l2.run === Seq(11, 21, 31)
      }
    }

    "when we want both lists, the shared computation must be computed only once" >> { implicit sc: ScoobiConfiguration =>
      val l1 = DList(1, 2, 3).map(_ * 10)
      val l2 = l1.map(_ + 1 )
      val doFn = new BasicDoFn[Int, Int] {
        var processedNumber = 0
        def process(input: Int, emitter: Emitter[Int]) {
          processedNumber += 1
          if (processedNumber > 3) failure("too many computations")
          emitter.emit(input + 2)
        }
      }
      val l3 = l1.parallelDo(doFn)


      "the list l1 has been computed only once" ==> {
        persist(l2, l3) must not(throwAn[Exception])
        (l2.run, l3.run) === (Seq(11, 21, 31), Seq(12, 22, 32))
      }
    }
  }
  end

  "2 objects and a list" >> { implicit sc: ScoobiConfiguration =>
    val list: DList[Int]    = DList(1, 2, 3)
    val plusOne: DList[Int] = list.map(_ + 1)

    // the sum of all values
    val sum: DObject[Int] = list.sum
    // the max of all values
    val max: DObject[Int] = list.max

    // execute the computation graph for the 2 DObjects and one DList
    persist(sum, max, plusOne)

    // collect results
    (sum.run, max.run, plusOne.run) === (6, 3, Vector(2, 3, 4))
  }
}
