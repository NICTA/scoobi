package com.nicta.scoobi.acceptance

import com.nicta.scoobi.testing.NictaHadoop
import com.nicta.scoobi.testing.mutable.SimpleJobs
import com.nicta.scoobi.DList

class SimpleMapReduceSpec extends NictaHadoop with SimpleJobs {

  "Issue #25" >> {
    "persisting a file without any transformation must not crash" >> { implicit c: SC =>
      fromInput("first", "example").run must_== Seq("first", "example")
    }
    "persisting a file with a simple map must not crash" >> { implicit c: SC =>
      fromInput(keepFiles = true)("second", "example").run { list: DList[String] => list.map(_.size) } must_== Seq("6", "7")
    }
  }
}

