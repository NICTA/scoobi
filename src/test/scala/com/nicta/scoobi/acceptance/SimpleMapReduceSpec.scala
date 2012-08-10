package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import impl.exec.JobExecException

class SimpleMapReduceSpec extends NictaSimpleJobs {

  section("issue #25")
  "persisting a file without any transformation must not crash" >> { implicit c: SC =>
    fromInput("first", "example").run must_== Seq("first", "example")
  }
  "persisting a file with a simple map must not crash" >> { implicit c: SC =>
    fromInput("second", "example").map(_.size).run must_== Seq("6", "7")
  }
  section("issue #25")

  tag("issue #83")
  "Concatenating multiple DLists results in a single DList with all elements" >> { implicit c: SC =>

    val xs = (1 to 4).toList
    val ys = (5 to 8).toList

    (xs.toDList ++ ys.toDList).run.sorted must_== (xs ++ ys).toSeq.sorted
  }

  section("issue #60")
  "Persisting an empty list shouldn't fail" >> { implicit c: SC =>
    val list = DList[String]()
    list.run must beEmpty
  }
  "Persisting a DList which becomes empty after filtering shouldn't fail" >> { implicit c: SC =>
    val list = DList(1 -> "one", 2 -> "two", 3 -> "three").filter(_ => false).groupByKey.groupByKey
    list.run must beEmpty
  }
  section("issue #60")

  tag("issue #75")
  "Concatenating to an empty list shouldn't fail" >> { implicit c: SC =>
    val list = DList[String]() ++ fromInput("hello", "world")
    list.run must_== Seq("hello", "world")
  }

  tag("issue #105")
  "Fail M/R job when task fails" >> { implicit sc: SC =>
    fromInput("hello", "world").
      map[String](_ => throw new RuntimeException("forcing a failure in the mapper")).run must throwA[JobExecException]
  }

  // tag all the example as "acceptance"
  // this way:
  // when "test-only -- include unit" is called, those tests won't be executed
  // a single issue can be re-run with "test-only -- include issue 83 && local
  override def acceptanceSection = section("acceptance")

}
case class MyMap(i: Int, s: String)
object MyMap {
  def apply(pair: (Int, String)) = new MyMap(pair._1, pair._2)
}
