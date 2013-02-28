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
import core.Reduction._
import testing.mutable.NictaSimpleJobs

class WordCountSpec extends NictaSimpleJobs {

  "Counting words frequencies must return the frequency for each word" >> { implicit sc: SC =>

    val frequencies =
      DList(repeat("hello" -> 3, "world" -> 4, "universe" -> 2):_*).
      mapFlatten(_.split(" ")).map((_, 1)).
      groupByKey.
      filter { case (word, n) => word.length < 6 }.
      combine(Sum.int)

    frequencies.run.sorted must_== Seq(("hello", 3), ("world", 4))

  }
  /** @return a Seq of strings where each key has been duplicated a number of times indicated by the value */
  def repeat(m: (String, Int)*): Seq[String] = m.flatMap { case (k, v) => Seq.fill(v)(k) }

}
