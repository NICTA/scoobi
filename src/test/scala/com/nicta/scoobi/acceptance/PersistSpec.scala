package com.nicta.scoobi
package acceptance

import testing.mutable.NictaSimpleJobs
import testing.{TestFiles, TempFiles}
import Scoobi._
import impl.plan.comp.CompNodeData
import CompNodeData._

class PersistSpec extends NictaSimpleJobs with ResultFiles {
  
  "There are many ways to execute computations with DLists or DObjects".txt

  "1. a single DList, simply add a sink, like a TextFile and persist the list" >> { implicit sc: ScoobiConfiguration =>
    val resultDir = TestFiles.createTempDir("test")
    val list = DList(1, 2, 3).toTextFile(resultDir.getPath, overwrite = true)

    list.persist
    resultDir must containResults
  }
  "2. a single DObject" >> { implicit sc: ScoobiConfiguration =>
    val o1 = DList(1, 2, 3).sum
    o1.run === 6
  }
  "3. a list, materialised" >> { implicit sc: ScoobiConfiguration =>
    val list = DList(1, 2, 3)
    list.run.normalise === "Vector(1, 2, 3)"
  }
  "4. a list, having a sink and also materialised" >> { implicit sc: ScoobiConfiguration =>
    val resultDir = TestFiles.createTempDir("test")
    val list = DList(1, 2, 3).toTextFile(resultDir.getPath, overwrite = true)

    resultDir must containResults
    list.run.normalise === "Vector(1, 2, 3)"
  }
  endp

  "5. Two materialised lists with a common ancestor" >> {
    "5.1 when we only want one list, only the computations for that list must be executed" >> { implicit sc: ScoobiConfiguration =>
      val l1 = DList(1, 2, 3).map(_ * 10)
      val l2 = l1.map(_ + 1 )
      val l3 = l1.map(i => { failure("l3 must not be computed"); i + 2 })

      "the list l1 is computed only once and l3 is not computed" ==> {
        l2.run must not(throwAn[Exception])
        l2.run.normalise === "Vector(11, 21, 31)"
      }
    }

    "5.2 when we want both lists, the shared computation must be computed only once" >> { implicit sc: ScoobiConfiguration =>
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
        (l2.run.normalise, l3.run.normalise) === ("Vector(11, 21, 31)", "Vector(12, 22, 32)")
      }
    }
    "5.3 when we iterate with several computations" >> { implicit sc: ScoobiConfiguration =>
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

  "6. 2 objects and a list" >> { implicit sc: ScoobiConfiguration =>
    val list: DList[Int]    = DList(1, 2, 3)
    val plusOne: DList[Int] = list.map(_ + 1)

    // the sum of all values
    val sum: DObject[Int] = list.sum
    // the max of all values
    val max: DObject[Int] = list.max

    // execute the computation graph for the 2 DObjects and one DList
    persist(sum, max, plusOne)

    // collect results
    (sum.run, max.run, plusOne.run.normalise) === (6, 3, "Vector(2, 3, 4)")
  }

  "7. A tuple containing 2 objects and a list" >> { implicit sc: ScoobiConfiguration =>
    val list: DList[Int]    = DList(1, 2, 3)
    val plusOne: DList[Int] = list.map(_ + 1)

    // execute the computation graph for the 2 DObjects and one DList
    val (sum, max, plus1) = run(list.sum, list.max, plusOne)
    (sum, max, plus1.normalise) === (6, 3, "Vector(2, 3, 4)")
  }

  "8. A user-specified sink must be used to save data when specified" >> { implicit sc: ScoobiConfiguration =>
    val sink = TestFiles.createTempDir("user")
    val plusOne = DList(1, 2, 3).map(_ + 1).toTextFile(sink.getPath)

    persist(plusOne)
    sink must containResults
  }

  "9. A user-specified sink can be used as a source when a second persist is done" >> {
    "9.1 with a sequence file" >> { implicit sc: ScoobiConfiguration =>
      persistTwice((list, sink) => list.convertValueToSequenceFile(sink, overwrite = true))
    }
    "9.2 with an Avro file" >> { implicit sc: ScoobiConfiguration =>
      persistTwice((list, sink) => list.toAvroFile(sink, overwrite = true))
    }
    "9.3 with a Text file" >> { implicit sc: ScoobiConfiguration =>
      persistTwice((list, sink) => list.toTextFile(sink, overwrite = true))
    }

  }

  "10. iterated computations" >> {
    "10.1 iterate 3 times on a DList while saving intermediate outputs" >> { implicit sc: ScoobiConfiguration =>
      val ints = DList(13, 5, 8, 11, 12)
      val out = TestFiles.createTempDir("out")
      def iterate(list: DList[Int]): DList[Int] = {
        persist(list)
        if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1).toAvroFile(out.getPath, overwrite = true))
        else                   list
      }
      normalise(iterate(ints).run) === "Vector(10, 2, 5, 8, 9)"
    }
  }

  "11. adding a sink on an already computed list" >> { implicit sc: ScoobiConfiguration =>
    val list = DList(1, 2, 3)
    val l2 = list.persist
    val out = TestFiles.createTempDir("out")
    l2.toTextFile(out.getPath).persist
    out must containResults
  }

  def persistTwice(withFile: (DList[Int], String) => DList[Int])(implicit sc: ScoobiConfiguration) = {
    val sink = TempFiles.createTempFilePath("user")
    val plusOne = withFile(DList(1, 2, 3).map(_ + 1), sink)

    persist(plusOne)

    val plusTwo = plusOne.map(_ + 2)
    normalise(plusTwo.run) === "Vector(4, 5, 6)"
  }
}
