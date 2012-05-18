package com.nicta.scoobi.acceptance

import com.nicta.scoobi.DList
import com.nicta.scoobi.testing.NictaSimpleJobs

class SimpleMapReduceSpec extends NictaSimpleJobs {

  "Issue #25" >> {
    "persisting a file without any transformation must not crash" >> { implicit c: SC =>
      fromInput("first", "example").run must_== Seq("first", "example")
    }
    "persisting a file with a simple map must not crash" >> { implicit c: SC =>
      fromInput("second", "example").run { list: DList[String] => list.map(_.size) } must_== Seq("6", "7")
    }
  }
}

