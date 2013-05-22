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
