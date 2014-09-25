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

import testing.script.NictaSimpleJobs
import Scoobi._
import com.nicta.scoobi.core.{Reduction, Counters}
import Reduction.Reduction._
import impl.plan.comp.CompNodeData._
import scala.collection.JavaConversions._
import org.specs2.specification.Groups
import com.nicta.scoobi.impl.Configurations

class CountersSpec extends NictaSimpleJobs with Groups { def is = sequential ^ s2"""

 It is possible to increment counters when doing DList operations
  + inside a map with a paralleDo
  + inside a reduce
  + across several hadoop jobs
  + to get the number of values per mapper
  + to get the number of values per reducer
  + no counter must be created by default

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

    eg := { implicit sc: SC =>
      sc.setCountValuesPerMapper(true)

      DList(("hello", 1), ("hello", 2), ("hi", 3), ("hi", 4), ("hi", 5)).groupByKey.combine(Sum.int).run
      val counts = sc.counters.getGroup(Configurations.MAPPER_VALUES_COUNTER).iterator.toSeq.map(c => (c.getName, c.getValue)).toSet

      if (sc.isLocal)       counts === Set(("mapper-0", 5))
      else if (sc.isRemote) counts === Set(("mapper-0",2), ("mapper-1",2), ("mapper-2",1))
      else                  ok
    }

    eg := { implicit sc: SC =>
      sc.setMinReducers(2)
      sc.setMaxReducers(2)
      sc.disableCombiners
      sc.setCountValuesPerReducer(true)

      DList(("hello", 1), ("hello", 1), ("hi", 1), ("hi", 1), ("hi", 1)).groupByKey.combine(Sum.int).run
      val counts = sc.counters.getGroup(Configurations.REDUCER_VALUES_COUNTER).iterator.toSeq.map(c => (c.getName, c.getValue)).toSet

      if (sc.isLocal)       counts === Set(("reducer-0", 5))
      else if (sc.isRemote) counts === Set(("reducer-0", 2), ("reducer-1", 3))
      else                  ok
    }

    eg := { implicit sc: SC =>
      sc.setCountValuesPerMapper(false)
      sc.setCountValuesPerReducer(true)

      DList(("hello", 1), ("hello", 2), ("hi", 3), ("hi", 4), ("hi", 5)).groupByKey.combine(Sum.int).run
      val counts = sc.counters.getGroup(Configurations.MAPPER_VALUES_COUNTER).iterator.toSeq
      counts must beEmpty
    }

  }
}
