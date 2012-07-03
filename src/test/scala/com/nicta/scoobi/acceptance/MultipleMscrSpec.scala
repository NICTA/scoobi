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
          "3,baz").collect { case AnInt(i) :: value :: _ => (i, value) }.lines

     val right =
      fromDelimitedInput(
          "2,chi",
          "4,qua",
          "5,tao").collect { case AnInt(i) :: value :: _ => (i, value) }.lines

    val j = left joinFullOuter right

    persist(j.length) must_== 5
  }
}
