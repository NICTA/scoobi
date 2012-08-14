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
}
