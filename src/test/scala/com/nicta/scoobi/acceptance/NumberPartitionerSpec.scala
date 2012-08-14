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
import org.specs2.matcher.Matcher

class NumberPartitionerSpec extends NictaSimpleJobs {

  "Numbers can be partitioned into even and odd numbers" >> { implicit sc: SC =>
    val numbers = fromInput((1 to count).map(i => r.nextInt(count * 2).toString):_*).map((_:String).toInt)
    val (evens, odds) = numbers.partition(_ % 2 == 0).run

    forall(evens.map(_.toInt))(i => i must beEven)
    forall(odds.map(_.toInt))(i => i must beOdd)
  }

  val r = new scala.util.Random
  val count = 50

  def beEven: Matcher[Int] = (i: Int) => (i % 2 == 0, i + " is not even")
  def beOdd = beEven.not
}
