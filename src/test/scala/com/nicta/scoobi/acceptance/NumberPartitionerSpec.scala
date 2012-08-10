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
