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

import testing.mutable.script.NictaSimpleJobs
import Scoobi._
import core.Counters
import impl.plan.comp.CompNodeData._

class CountersSpec extends NictaSimpleJobs { s2"""

 It is possible to increment counters when doing DList operations
  + inside a map with a paralleDo
  + inside a reduce
  + across several hadoop jobs

"""

  "counters" - new group {
    eg := {
      implicit sc: SC =>
      val list = DList(1, 2, 3).map((i: Int) => i + 1).parallelDo((input: Int, counters: Counters) => {
        counters.incrementCounter("group1", "counter1", 1)
        input + 1
      })
      list.run.normalise === "Vector(3, 4, 5)"
      sc.counters.getGroup("group1").findCounter("counter1").getValue must be_==(3).when(sc.isLocal)
    }

    eg := { implicit sc: SC =>
      val list = DList(1, 2, 3).map((i: Int) => (i, i)).groupByKey.parallelDo((input: (Int, Iterable[Int]), counters: Counters) => {
        counters.incrementCounter("group1", "counter1", 1)
        input
      })
      list.run
      sc.counters.getGroup("group1").findCounter("counter1").getValue must be_==(3).when(sc.isLocal)
    }

    eg := { implicit sc: SC =>
      // increment counters in first map, then in the reducer after the second gbk
      val list = DList(1, 2, 3).parallelDo((input: Int, counters: Counters) => {
        counters.incrementCounter("group1", "counter1", 1)
        input + 1
      }).map((i: Int) => (i, i)).
        groupByKey.map { case (k, v) => (k, k) }.
        groupByKey.parallelDo((input: (Int, Iterable[Int]), counters: Counters) => {
        counters.incrementCounter("group1", "counter1", 1)
        input
      })

      list.run
      sc.counters.getGroup("group1").findCounter("counter1").getValue must be_==(6).when(sc.isLocal)
    }
  }
}
