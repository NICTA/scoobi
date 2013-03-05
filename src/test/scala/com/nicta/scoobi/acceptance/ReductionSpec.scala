package com.nicta.scoobi
package acceptance

import com.nicta.scoobi.testing.mutable.NictaSimpleJobs
import core.Reduction, Reduction._
import scalaz._, Scalaz._

class ReductionSpec extends NictaSimpleJobs {

  "option reduction" >> {
    val r: Reduction[Option[String]] =
      string.option
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    val c: Option[String] =
      r.reduce(None, Some("def"))
    val d: Option[String] =
      r.reduce(None, None)
    (a, b, c, d) === (Some("abcdef"), Some("abc"), Some("def"), None)
  }

  "first option reduction" >> {
    val r: Reduction[Option[String]] =
      firstOption
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    val c: Option[String] =
      r.reduce(None, Some("def"))
    val d: Option[String] =
      r.reduce(None, None)
    (a, b, c, d) === (Some("abc"), Some("abc"), Some("def"), None)
  }

  "last option reduction" >> {
    val r: Reduction[Option[String]] =
      lastOption
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    val c: Option[String] =
      r.reduce(None, Some("def"))
    val d: Option[String] =
      r.reduce(None, None)
    (a, b, c, d) === (Some("def"), Some("abc"), Some("def"), None)
  }

  "dual reduction" >> {
    val r: Reduction[List[String]] =
      list.dual
    val a =
      r.reduce(List("abc", "def"), List("xyz"))
    a === List("xyz", "abc", "def")
  }

  "zip int addition and string reduction" >> {
    // Zip the integer addition reduction with the string append reduction.
    // This creates a reduction on the pair of Int and String.
    val r: Reduction[(Int, String)] =
      Sum.int zip string
    // Apply values to the paired reduction.
    val a: (Int, String) =
      r.reduce((7, "abc"), (8, "def"))
    // Assert the result of the reduction.
    // The integer addition reduction is performed on the _1 side of the pair.
    // The string append reduction is performed on the _2 side of the pair.
    a === (15, "abcdef")
  }

  "endomorphism reduction" >> {
    // Endo — prefix meaning, "within" or "onto" (Greek).
    // morphism — meaning, "transformation" or mapping between structure.
    // Endomorphism — a mapping that preserves structure within itself.
    //              — for Scala, a function that accepts an argument of some type and and returns that same type.

    // Construct a reduction on Int => Int
    val r: Reduction[Int => Int] =
      endo
    // Apply the reduction to two endomorphic mappings.
    // The -\ method is used, which is an alias for reduce.
    val a: Int => Int =
      r -\ (10+, 2*)
    // Assert the result of the reduction.
    // Performs *2, then +10 on 44 == 98.
    a(44) === 98
  }

  "pointwise2 reduction" >> {
    // Takes the list append reduction to a function accepting Int to list.
    // The pointwise reduction runs the reduction of the return value; in this case list append.
    val r: Reduction[Int => List[String]] =
      list.pointwise
    // Apply the reduction to a pair of functions: Int => List[String]
    val a: Int => List[String] =
      r(n => List(n.toString, n.toString.reverse), n => List((n * 2).toString))
    // Assert the result of the reduction, which applies the integer 456
    // and appends the resulting lists.
    a(456) === List("456", "654", "912")
  }

  "pointwiseK reduction" >> {
    val r: Reduction[Kleisli[Option, Int, String]] =
      string.pointwiseK[Option, Int]
    val a: Kleisli[Option, Int, String] =
      r.reduce(Kleisli(n => Some(n.toString)), Kleisli(n => Some((n * 10).toString)))
    val b =
      r.reduce(Kleisli(n => Some(n.toString)), Kleisli(_ => None))
    (a(43), b(43)) === (Some("43430"), None)
  }

  "pointwiseC reduction" >> {
    val r: Reduction[Cokleisli[Option, Int, String]] =
      string.pointwiseC[Option, Int]
    val a: Cokleisli[Option, Int, String] =
      r.reduce(Cokleisli(_ match {
        case None => "abc"
        case Some(n) => n.toString
      }), Cokleisli(_ match {
        case None => "def"
        case Some(n) => (n * 10).toString
      }))

    (a run Some(43), a run None) === ("43430", "abcdef")
  }

  "store reduction" >> {
    val r: Reduction[Store[List[Char], String]] =
      list store string
    val a: Store[List[Char], String] =
      r(Store(_.reverse.mkString, List('a', 'b')), Store(_.map(_.toUpper).mkString, List('c', 'd')))
    (a put List('x', 'y', 'z'), a.pos) === ("zyxXYZ", List('a', 'b', 'c', 'd'))
  }

  "state reduction" >> {
    val r: Reduction[State[Int, String]] =
      string.state
    val a: State[Int, String] =
      r(State(n => (n + 10, "abc" + n)), State(n => (n * 2, "def")))
    a(11) === (42, "abc11def")
  }

  "reduction to applicative environment (apply)" >> {
    val r: Apply[({type lam[a]=String})#lam] =
      string.apply
    val a: String =
      r.ap("def")("abc")
    a === "abcdef"
  }

  "reduction to composition environment (compose)" >> {
    val r: Compose[({type lam[a,b]=String})#lam] =
      string.compose
    val a: String =
      r.compose("abc", "def")
    a === "abcdef"
  }

  "comparable reduction" >> {
    val r: Reduction[Comparable[Int]] =
      comparable
    val a: Comparable[Int] =
      r(7, 8)
    List((6, true), (7, true), (8, false), (9, false)) forall {
      case (n, p) => {
        val w = a.compareTo(n)
        if(p)
          w > 0
        else
          w < 0
      }

    }
  }

}
