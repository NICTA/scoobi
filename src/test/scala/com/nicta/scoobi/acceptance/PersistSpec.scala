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

import testing.mutable.NictaSimpleJobs
import testing.{InputStringTestFile, TestFiles, TempFiles}
import Scoobi._
import impl.plan.comp.CompNodeData
import CompNodeData._
import TestFiles._
import io.text.TextSource
import org.apache.hadoop.mapreduce.RecordReader
import core.{InputConverter, InputOutputContext}
import impl.plan.DListImpl
import org.apache.hadoop.io.{Text, LongWritable}
import scala.io.Source
import org.specs2.mutable.Tags

class PersistSpec extends NictaSimpleJobs with ResultFiles { sequential

  "There are many ways to execute computations with DLists or DObjects".txt

  "1. a single DList, simply add a sink, like a TextFile and persist the list" >> { implicit sc: SC =>
    val resultDir = createTempDir("test")
    val list = DList(1, 2, 3).toTextFile(path(resultDir), overwrite = true)

    list.persist
    resultDir must containResults
  }
  "2. a single DObject" >> { implicit sc: SC =>
    val o1 = DList(1, 2, 3).sum
    o1.run === 6
  }
  "3. a list, materialised" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)
    list.run.normalise === "Vector(1, 2, 3)"
  }
  "4. a list, having a sink and also materialised" >> { implicit sc: SC =>
    val resultDir = createTempDir("test")
    val list = DList(1, 2, 3).toTextFile(path(resultDir), overwrite = true)
    list.run.normalise === "Vector(1, 2, 3)"

    resultDir must containResults
    list.run.normalise === "Vector(1, 2, 3)"
  }
  endp

  "5. Two materialised lists with a common ancestor" >> {
    "5.1 when we only want one list, only the computations for that list must be executed" >> { implicit sc: SC =>
      val l1 = DList(1, 2, 3).map(_ * 10)
      val l2 = l1.map(_ + 1 )
      val l3 = l1.map(i => { failure("l3 must not be computed"); i + 2 })

      "the list l1 is computed only once and l3 is not computed" ==> {
        l2.run must not(throwAn[Exception])
        l2.run.normalise === "Vector(11, 21, 31)"
      }
    }

    "5.2 when we want both lists, the shared computation must be computed only once" >> { implicit sc: SC =>
      val l1 = DList(1, 2, 3).map(_ * 10)
      val l2 = l1.map(_ + 1)

      var processedNumber = 0

      val doFn = (input: Int, emitter: Emitter[Int]) => {
        processedNumber += 1
        if (processedNumber > 3) failure("too many computations")
        emitter.emit(input + 2)
      }

      val l3 = l1.parallelDo(doFn)

      "the list l1 has been computed only once" ==> {
        persist(l2, l3) must not(throwAn[Exception])
        (l2.run.normalise, l3.run.normalise) === ("Vector(11, 21, 31)", "Vector(12, 22, 32)")
      }
    }

    "5.3 when we iterate with several computations" >> { implicit sc: SC =>
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

  "6.2 objects and a list" >> { implicit sc: SC =>
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

  "7. A tuple containing 2 objects and a list" >> { implicit sc: SC =>
    val list: DList[Int]    = DList(1, 2, 3)
    val plusOne: DList[Int] = list.map(_ + 1)

    // execute the computation graph for the 2 DObjects and one DList
    val (sum, max, plus1) = run(list.sum, list.max, plusOne)
    (sum, max, plus1.normalise) === (6, 3, "Vector(2, 3, 4)")
  }

  "8. A user-specified sink must be used to save data when specified" >> { implicit sc: SC =>
    val sink = createTempDir("user")
    val plusOne = DList(1, 2, 3).map(_ + 1).toTextFile(path(sink))

    persist(plusOne)
    sink must containResults
  }

  "9. A user-specified sink can be used as a source when a second persist is done" >> {
    "9.1 with a sequence file" >> { implicit sc: SC =>
      persistTwice((list, sink) => list.valueToSequenceFile(sink, overwrite = true))
    }
    "9.2 with an Avro file" >> { implicit sc: SC =>
      persistTwice((list, sink) => list.toAvroFile(sink, overwrite = true))
    }
    "9.3 with a Text file" >> { implicit sc: SC =>
      // in this case, a bridge store is used as a source, instead of the text file
      persistTwice((list, sink) => list.toTextFile(sink, overwrite = true))
    }

  }

  "10. iterated computations" >> { implicit sc: SC =>
    val ints = DList(13, 5, 8, 11, 12)
    val out = createTempDir("out")
    def iterate(list: DList[Int]): DList[Int] = {
      persist(list)
      val m = list.max.run
      if (m > 10) iterate(list.map(i => if (i <= 0) i else i - 1).toAvroFile(path(out), overwrite = true))
      else                   list
    }
    normalise(iterate(ints).run) === "Vector(10, 2, 5, 8, 9)"
  }

  "11. adding a sink on an already computed list" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)
    val l2 = list.persist
    val out = TestFiles.createTempDir("out")
    l2.toTextFile(path(out)).persist
    out must containResults
  }

  "12. It is possible to run the same list both in memory and with another context. The results should be the same" >> { implicit sc: SC =>
    val list = DList(1, 2, 3)
    normalise(list.run) === normalise(list.run(configureForInMemory(ScoobiConfiguration())))
  }


  "13. 1 combine + 1 persist + 1 combine" >> {implicit sc: SC =>
    val l1 = DList(1, 2, 1, 2).map(x => (x % 2, x)).groupByKey.combine(Reduction.Sum.int)
    l1.run
    l1.map(_._2).run.normalise === "Vector(2, 4)"
  }

  "14. complex flatten + gbk" >> {implicit sc: SC =>
    val l1 = DList(1, 2, 3)
    val l2 = (DList(1).sum join DList(4, 5, 6)).map(_._2)
    val l3 = (l1 ++ l2).map(x => (x % 2, x)).groupByKey.combine(Reduction.Sum.int)
    l3.map(_._2).run.normalise === "Vector(12, 9)"
  }

  "15. possible truncate issue when new nodes are added and an intermediary list is persisted" >> { implicit sc: SC =>
    val l1 = DList(1, 2, 3)
    val l2 = l1.map(_+1)
    l1.run
    l2.map(_+2).run.normalise === "Vector(4, 5, 6)"
  }

  "16. issue 213" >> { implicit sc: SC =>
    val r = scala.util.Random
    val numbers  = DList(1, 2).map { i => r.nextInt(i*10) }
    val numbers2 = numbers.map(identity)
    val (a, b) = run(numbers.sum, numbers2.sum)
    a must be_==(b)
  }

  "17. issue 215" >> { implicit sc: SC =>
    val dir = TempFiles.createTempDir("test.avro")
    persist((1 to 2).toDList.map { x => x.toString -> x.toString }.toAvroFile(dir.getPath, true))

    val input = fromAvroFile[(String, String)](dir.getPath)
    (input.materialise join input.groupByKey).run.normalise === "Vector((Vector((1,1), (2,2)),(1,Vector(1))), (Vector((1,1), (2,2)),(2,Vector(2))))"
  }

  "18. issue 216" >> { implicit sc: SC =>
    val input = (1 to 2).toDList
    (input.materialise join input).run.normalise === "Vector((Vector(1, 2),1), (Vector(1, 2),2))"
  }

  "19. issue 175: it is possible to store a DObject[A]" >> { implicit sc: SC =>
    val list = DList(1, 2)
    val dir = TempFiles.createTempDir("test")
    val o1: DObject[Int] = list.sum.toTextFile(path(dir.getPath))
    persist(o1)
    dirResults(sc)(dir) === Seq("3")
  }

  section("issue 175")
  "20. issue 175: storing and loading DObjects" >> {

    "it is possible to store a DObject[Iterable[A]] with a Text file" >> { implicit sc: SC =>
      val list = DList(1, 2)
      val dir = TempFiles.createTempDir("test")
      val o1: DObject[Iterable[Int]] = list.materialise.toTextFile(path(dir.getPath))
      persist(o1)
      dirResults(sc)(dir).normalise === "Vector(1, 2)"
    }

    "it is possible to store a DObject[A] with a Sequence file" >> { implicit sc: SC =>
      val list = DList(1, 2)
      val dir1 = TempFiles.createTempDir("seq")
      val o1 = list.sum.keyToSequenceFile(path(dir1.getPath))
      persist(o1)

      objectKeyFromSequenceFile[Int](path(dir1.getPath)).run === 3
    }

    "it is possible to store a DObject[A] with an Avro file" >> { implicit sc: SC =>
      val list = DList(1, 2)
      val dir1 = TempFiles.createTempDir("avro")
      val o1 =  list.sum.toAvroFile(path(dir1.getPath))
      persist(o1)

      objectFromAvroFile[Int](path(dir1.getPath)).run === 3
    }

    "it is possible to read a DObject[A] from an Avro file" >> { implicit sc: SC =>
      val list = DList(1, 2)
      val (dir1, dir2) = (TempFiles.createTempDir("avro-in"), TempFiles.createTempDir("avro-out"))
      list.sum.toAvroFile(path(dir1.getPath)).run
      val data = objectFromAvroFile[Int](path(dir1.getPath)).map(_ + 1)
      persist(data.toTextFile(path(dir2.getPath)))
      data.run === 4
    }

    "it is possible to read a DObject[Iterable[A]] from an Avro file" >> { implicit sc: SC =>
      val list = DList(1, 2)
      val (dir1, dir2) = (TempFiles.createTempDir("avro-in"), TempFiles.createTempDir("avro-out"))
      list.toAvroFile(path(dir1.getPath)).run
      val data = fromAvroFile[Int](path(dir1.getPath)).map(_ + 1)
      persist(data.toTextFile(path(dir2.getPath)))
      data.run.normalise === "Vector(2, 3)"
    }
  }
  section("issue 175")

  section("issue 288")
  "a DObject created from a path must not trigger a mapreduce job for its evaluation" >> {
    "from a Text File" >> { implicit sc: SC =>
      val file = path(TempFiles.createTempFilePath("test"))
      TempFiles.writeLines(file, Seq("hello\tworld", "this\tis\tme"), sc.isRemote)
      objectFromTextFile(file).run === "hello\tworld"
    }
    "back to a DList" >> { implicit sc: SC =>
      val file = path(TempFiles.createTempFilePath("test-back"))
      TempFiles.writeLines(file, Seq("hello\tworld", "this\tis\tme"), sc.isRemote)
      (objectFromTextFile(file).map(_.split("\t").toSeq.head) join DList(1, 2)).run === Seq(("hello", 1), ("hello", 2))
    }
    "from a delimited Text File " >> { implicit sc: SC =>
      val file = path(TempFiles.createTempFilePath("test-delimited"))
      TempFiles.writeLines(file, Seq("hello\tworld", "this\tis\tme"), sc.isRemote)
      objectFromDelimitedTextFile(file) { case values => values.mkString(" ") }.run === "hello world"
    }
    "from a Sequence file" >> {
      "get the key" >> { implicit sc: SC =>
        val file = path(TempFiles.createTempFilePath("test-seq-key"))
        DList((1, "2")).toSequenceFile(file).persist
        objectKeyFromSequenceFile[Int](file).run === 1
      }
      "get the value" >> { implicit sc: SC =>
        val file = path(TempFiles.createTempFilePath("test-seq-value"))
         DList((1, "2")).toSequenceFile(file).persist
         objectValueFromSequenceFile[String](file).run === "2"
      }
      "get the (key, value)" >> { implicit sc: SC =>
        val file = path(TempFiles.createTempFilePath("test-seq-key-value"))
        DList((1, "2")).toSequenceFile(file).persist
        objectFromSequenceFile[Int, String](file).run === (1, "2")
      }
    }
  }
  section("issue 288")

  def persistTwice(withFile: (DList[Int], String) => DList[Int])(implicit sc: SC) = {
    val sink = TempFiles.createTempFilePath("user")
    val plusOne = withFile(DList(1, 2, 3).map(_ + 1), sink)

    persist(plusOne)

    val plusTwo = plusOne.map(_ + 2)
    normalise(plusTwo.run) === "Vector(4, 5, 6)"
  }

}
