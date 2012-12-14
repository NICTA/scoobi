package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import testing.TempFiles
import Scoobi._

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
    "when we only want one list, only the computations for that list must be executed" >> { implicit sc: ScoobiConfiguration =>
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
      val l2 = l1.map(_ + 1)

      var processedNumber = 0
      val doFn = new BasicDoFn[Int, Int] {
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
    "when we iterate with several computations" >> { implicit sc: ScoobiConfiguration =>
      var list: DList[(Int, Int)] = DList((1, 1))

      list.map(_._1).run
      list = list.groupByKey.map(x => (1, 1))
      list.map(_._1).run
      list = list.groupByKey.map(x => (1, 1))
      list.map(_._1).run
      ok
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

  "A tuple containing 2 objects and a list" >> { implicit sc: ScoobiConfiguration =>
    val list: DList[Int]    = DList(1, 2, 3)
    val plusOne: DList[Int] = list.map(_ + 1)

    // execute the computation graph for the 2 DObjects and one DList
    run(list.sum, list.max, plusOne) === (6, 3, Vector(2, 3, 4))
  }
}
