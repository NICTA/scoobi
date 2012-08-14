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

class MultipleMscrSpec extends NictaSimpleJobs {

  "A 'join' followed by a 'reduction' should work" >> { implicit c: SC =>
    val left =
      fromDelimitedInput(
          "1,foo",
          "2,bar",
          "3,baz").collect { case AnInt(i) :: value :: _ => (i, value) }

     val right =
      fromDelimitedInput(
          "2,chi",
          "4,qua",
          "5,tao").collect { case AnInt(i) :: value :: _ => (i, value) }

    (left joinFullOuter right).length.run must_== 5
  }


  "An MSCR can read from two intermediate outputs." >> { implicit c: SC =>

    def unique[A : Manifest : WireFormat : Grouping](x: DList[A]) = x.groupBy(identity).combine((a: A, b: A) => a)

    val words1 = List("hello", "world")
    val words2 = List("foo", "bar", "hello")

    val input1 = fromInput(Seq.fill(100)(words1).flatten: _*)
    val input2 = fromInput(Seq.fill(100)(words2).flatten: _*)

    /* The uniques will be interemediate outputs that feed into 'join' which will
     * be implemented by a separate MSCR.*/
    (unique(input1) join unique(input2)).run must_== Seq(("hello", ("hello", "hello")))
  }

  "A DList grouped in two different ways with one of them materialized and then joined to the other should work." >> { implicit c: SC =>

    val input = fromDelimitedInput("k1,v1","k2,v2").collect { case key :: value :: _ => (key, value) }

    val inputGrouped = input.groupBy(_._1)
    val inputGroupedDifferently = input.groupBy(_._2)

    val inputGroupedAsDObject = inputGrouped.materialize

    val dObjectJoinedToInputGroupedDiff = (inputGroupedAsDObject join inputGroupedDifferently)

    val expectedGBKs = Seq(("k1",Seq(("k1","v1"))), ("k2",Seq(("k2","v2"))))

    persist(dObjectJoinedToInputGroupedDiff.materialize).toSeq must_== Seq(
        (expectedGBKs, ("v1",Seq(("k1","v1")))),
        (expectedGBKs, ("v2",Seq(("k2","v2")))))
  }

  "A DList grouped in two different ways with one of them grouped again, materialized, then joined to the other." >> { implicit c: SC =>

    val input = fromDelimitedInput("k1,v1","k2,v2").collect { case key :: value :: _ => (key, value) }

    val inputGrouped = input.groupBy(_._1).map(a => a).groupBy(_._1).map(b => (b._1, b._2.flatMap(_._2)) )
    val inputGroupedDifferently = input.groupBy(_._2)

    val inputGroupedAsDObject = inputGrouped.materialize

    val dObjectJoinedToInputGroupedDiff = (inputGroupedAsDObject join inputGroupedDifferently)

    val expectedGBKs = Seq(("k1",Seq(("k1","v1"))), ("k2",Seq(("k2","v2"))))

    persist(dObjectJoinedToInputGroupedDiff.materialize).toSeq must_== Seq(
        (expectedGBKs, ("v1",Seq(("k1","v1")))),
        (expectedGBKs, ("v2",Seq(("k2","v2")))))
  }
}
