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

import Scoobi._
import testing.NictaSimpleJobs
import impl.exec.JobExecException

class SimpleMapReduceSpec extends NictaSimpleJobs {

  section("issue #25")
  "persisting a file without any transformation must not crash" >> { implicit c: SC =>
    fromInput("first", "example").run must_== Seq("first", "example")
  }
  "persisting a file with a simple map must not crash" >> { implicit c: SC =>
    fromInput("second", "example").map(_.size).run must_== Seq(6, 7)
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
  "Fail M/R job when task fails" >> { implicit c: SC =>
     fromInput("hello", "world").
        map[String](_ => throw new RuntimeException("forcing a failure in the mapper")).run must throwA[JobExecException].unless(c.isInMemory)
  }

  "We can materialise a DList with more than 1 reducer" >> { implicit c: SC =>
    c.setMinReducers(2)
    val xs = (1 to 10).toList
    xs.toDList.run.sorted must_== xs.toSeq
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
