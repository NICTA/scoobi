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
package core

import Scoobi._
import testing.mutable.NictaSimpleJobs
import org.specs2.mock.Mockito
class InMemoryDListSpec extends NictaSimpleJobs with Mockito {
  val shared = DList(
    "1,yes",
    "2,no",
    "3,no",
    "4,yes",
    "5,no",
    "6,yes",
    "7,yes"
  )

  "A shared dlist must work in memory (see next)" >> {
    val dlist = tryInt(split(shared))
    dlist.run(c).toSet must_== Set(
      ("1", Some(1)),
      ("2", Some(0)),
      ("3", Some(0)),
      ("4", Some(1)),
      ("5", Some(0)),
      ("6", Some(1)),
      ("7", Some(1))
    )
  }

  "A shared dlist must work in memory (see previous)" >> {
    val dlist = tryBoolean(split(shared))
    dlist.run(c).toSet must_== Set(
      ("1", Some(true)),
      ("2", Some(false)),
      ("3", Some(false)),
      ("4", Some(true)),
      ("5", Some(false)),
      ("6", Some(true)),
      ("7", Some(true))
    )
  }

  def c = configureForInMemory(ScoobiConfiguration())

  def split(data : DList[String]): DList[(String, String)] =
    data.map(line => line.split(",").toList match {
      case List(a, b) => (a, b)
      case _ => ("error", line)
    })

  def tryBoolean(data : DList[(String, String)]): DList[(String, Option[Boolean])] =
    data.map({
      case (k, "yes") => (k, Some(true))
      case (k, "no") => (k, Some(false))
      case (k, _) => (k, None)
    })

  def tryInt(data : DList[(String, String)]): DList[(String, Option[Int])] =
    tryBoolean(data).map({
      case (k, v) => (k, v.map(b => if (b) 1 else 0))
    })
}
