package com.nicta.scoobi.acceptance

import com.nicta.scoobi.{Scoobi, DList}
import Scoobi._
import com.nicta.scoobi.testing.NictaSimpleJobs

class SimpleMapReduceSpec extends NictaSimpleJobs {

  section("issue #25")
  "persisting a file without any transformation must not crash" >> { implicit c: SC =>
    fromInput("first", "example").run must_== Seq("first", "example")
  }
  "persisting a file with a simple map must not crash" >> { implicit c: SC =>
    fromInput("second", "example").run { list: DList[String] => list.map(_.size) } must_== Seq("6", "7")
  }
  section("issue #25")

  tag("issue #83")
  "Concatenating multiple DLists results in a single DList with all elements" >> { c: SC =>

    val xs = (1 to 4).toList
    val ys = (5 to 8).toList

    persist(c)((xs.toDList ++ ys.toDList).materialize).toSeq.sorted must_== (xs ++ ys).toSeq.sorted
  }

  // tag all the example as "acceptance"
  // this way:
  // when "test-only -- include unit" is called, those tests won't be executed
  // a single issue can be re-run with "test-only -- include "issue 83,local"
  override def acceptanceSection = section("acceptance")
}

