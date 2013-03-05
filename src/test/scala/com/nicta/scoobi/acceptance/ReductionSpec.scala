package com.nicta.scoobi
package acceptance

import com.nicta.scoobi.testing.mutable.NictaSimpleJobs
import core.Reduction, Reduction._
import scalaz._

class ReductionSpec extends NictaSimpleJobs {

  "zip int addition and string reduction" >> {
    // Zip the integer addition reduction with the string append reduction.
    // This creates a reduction on the pair of Int and String.
    val r: Reduction[(Int, String)] = Sum.int zip string
    // Apply values to the paired reduction.
    val x = r.reduce((7, "abc"), (8, "def"))
    // Assert the result of the reduction.
    // The integer addition reduction is performed on the _1 side of the pair.
    // The string append reduction is performed on the _2 side of the pair.
    x === (15, "abcdef")
  }

  "endomorphism reduction" >> {
    // Endo — prefix meaning, "within" or "onto" (Greek).
    // morphism — meaning, "transformation" or mapping between structure.
    // Endomorphism — a mapping that preserves structure within itself.
    //              — for Scala, a function that accepts an argument of some type and and returns that same type.

    // Construct a reduction on Int => Int
    val r: Reduction[Int => Int] = endo[Int]
    // Apply the reduction to two endomorphic mappings.
    // The -\ method is used, which is an alias for reduce.
    val x = r -\ (10+, 2*)
    // Assert the result of the reduction.
    // Performs *2, then +10 on 44 == 98.
    x(44) === 98
  }

  "pointwise2 reduction" >> {
    // Takes the list append reduction to a function accepting Int to list.
    // The pointwise reduction runs the reduction of the return value; in this case list append.
    val r: Reduction[Int => List[String]] = list.pointwise[Int]
    // Apply the reduction to a pair of functions Int => List[String]
    val x = r(n => List(n.toString, n.toString.reverse), n => List((n * 2).toString))
    // Assert the result of the reduction, which applies the integer 456
    // and appends the resulting lists.
    x(456) === List("456", "654", "912")
  }
}
