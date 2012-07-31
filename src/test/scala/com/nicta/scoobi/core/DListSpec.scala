package com.nicta.scoobi
package core

import org.apache.hadoop.io.Text
import Scoobi._
import testing.NictaSimpleJobs

class DListSpec extends NictaSimpleJobs {

  tag("issue 99")
  "a DList can be created and persisted with some Text" >> { implicit sc: SC =>
    val list = DList((new Text("key1"), new Text("value1")), (new Text("key2"), new Text("value2")))
    run(list).sorted must_== Seq("(key1,value1)", "(key2,value2)")
  }

  tag("issue 104")
  "Summing up an empty list should do something graceful" >> { implicit c: SC =>
    persist(DList[Int]().sum) must throwAn[Exception](message = "the reduce operation is called on an empty list")
  }
}
