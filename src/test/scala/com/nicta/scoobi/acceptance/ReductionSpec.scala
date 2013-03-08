package com.nicta.scoobi
package acceptance

import com.nicta.scoobi.testing.mutable.NictaSimpleJobs
import core.Reduction, Reduction._
import scalaz._, Scalaz._

class ReductionSpec extends NictaSimpleJobs {
  // Produce a reduction on option of string from a reduction on string.
  // The option reduction will take as many Some values as possible
  // and then reducing in the event of two Some values.
  "option reduction" >> {
    // Reduction on Option[String] from a reduction on String.
    val r: Reduction[Option[String]] =
      string.option
    // Reduce two Some values, which will reduce their inner values.
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    // Reduce one Some value, which will take that value.
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    // Reduce one Some value, which will take that value.
    val c: Option[String] =
      r.reduce(None, Some("def"))
    // Reduce no Some values, which will reduce to no value.
    val d: Option[String] =
      r.reduce(None, None)
    // Assert the results of the previous reductions.
    // As many Some values as possible has been taken by each reduction.
    (a, b, c, d) === (Some("abcdef"), Some("abc"), Some("def"), None)
  }

  // A reduction that will take the first Some value, starting at the one appearing left-most.
  "first option reduction" >> {
    val r: Reduction[Option[String]] =
      firstOption
    // Reduce on two Some values, which will take the one on the left.
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    // Reduce on one Some value, which will take that value.
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    // Reduce on one Some value, which will take that value.
    val c: Option[String] =
      r.reduce(None, Some("def"))
    // Reduce on no Some values, which will reduce to no value.
    val d: Option[String] =
      r.reduce(None, None)
    // Assert the results of the previous reductions.
    // As many left-most Some values as possible has been taken by each reduction.
    (a, b, c, d) === (Some("abc"), Some("abc"), Some("def"), None)
  }

  // A reduction that will take the first Some value, starting at the one appearing right-most.
  "last option reduction" >> {
    val r: Reduction[Option[String]] =
      lastOption
    // Reduce on two Some values, which will take the one on the right.
    val a: Option[String] =
      r.reduce(Some("abc"), Some("def"))
    // Reduce on one Some value, which will take that value.
    val b: Option[String] =
      r.reduce(Some("abc"), None)
    // Reduce on one Some value, which will take that value.
    val c: Option[String] =
      r.reduce(None, Some("def"))
    // Reduce on no Some values, which will reduce to no value.
    val d: Option[String] =
      r.reduce(None, None)
    // Assert the results of the previous reductions.
    // As many right-most Some values as possible has been taken by each reduction.
    (a, b, c, d) === (Some("def"), Some("abc"), Some("def"), None)
  }

  // Take the dual reduction of the list reduction.
  // The list reduction performs append and its dual appends with arguments in the opposite order.
  // That is, reduce(x, y) produces the result of y ++ x.
  "dual reduction" >> {
    // Construct the dual of the list reduction.
    val r: Reduction[List[String]] =
      list.dual
    // Reduce two list values.
    val a =
      r.reduce(List("abc", "def"), List("xyz"))
    // Assert the result of the reduction where the lists are appended in opposing order.
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

  // pointwiseK reduction performs reduction on a function's result type
  // within some applicative environment.
  "pointwiseK reduction" >> {
    // Construct a reduction on the argument type of Int and result type of String
    // within the Option applicative environment.
    val r: Reduction[Kleisli[Option, Int, String]] =
      string.pointwiseK
    // Apply the reduction to two kleisli functions that always produce a Some value.
    val a: Kleisli[Option, Int, String] =
      r.reduce(Kleisli(n => Some(n.toString)), Kleisli(n => Some((n * 10).toString)))
    // Apply the reduction to two functions where one produces a None value.
    val b: Kleisli[Option, Int, String] =
      r.reduce(Kleisli(n => Some(n.toString)), Kleisli(_ => None))
    // Assert the results of the two previous reductions.
    // The first reduction results in Some, per the Option applicative.
    // The function argument 43 is applied and the return values in the Some are reduced.
    // The second reduction results in None, since one of the functions produces None.
    // The Option applicative (monad) environment requires that Some values are produced
    // in order to result in a Some value. i.e. The first None causes a None result.
    (a(43), b(43)) === (Some("43430"), None)
  }

  // pointwiseC reduction performs reduction on a function's argument type
  // within some environment.
  "pointwiseC reduction" >> {
    // Construct a reduction on the result type of String and argument type of Int.
    val r: Reduction[Cokleisli[Option, Int, String]] =
      string.pointwiseC
    // Apply the reduction to two cokleisli functions that switch on the Option argument.
    val a: Cokleisli[Option, Int, String] =
      r.reduce(Cokleisli(_ match {
        case None => "abc"
        case Some(n) => n.toString
      }), Cokleisli(_ match {
        case None => "def"
        case Some(n) => (n * 10).toString
      }))

    // Assert the result of the previous reduction to two Option values.
    // The first reduction applies to a Some(43) value which results in
    // two strings "43" and "430" which are then further reduced (append).
    // The second reduction applies to a None value which results in
    // two strings "abc" and "def" which are then further reduced (append).
    (a run Some(43), a run None) === ("43430", "abcdef")
  }

  // The store reduction applies to the pair of a function (A => B) and a value (A).
  "store reduction" >> {
    // Produce a reduction on a store from the pair of reductions of list append and string append.
    // The store is the pair of values:
    //   * a function: (List[Char] => String)
    //   * a value: List[Char]
    val r: Reduction[Store[List[Char], String]] =
      list store string
    // Apply the reduction to a pair of store values.
    val a: Store[List[Char], String] =
      r(Store(_.reverse.mkString, List('a', 'b')), Store(_.map(_.toUpper).mkString, List('c', 'd')))
    // Assert the result of the reduction by examining the store.
    // The resulting store function is applied to an arbitrary list (put)
    // and its value is extracted (pos).
    // The arbitrary list ("xyz") is reversed on one side ("zyx") and converted to upper-case on the other ("XYZ").
    // After conversion to string, reduction on the pair of values occurs resulting in "zyxXYZ".
    // The store values are also reduced with list append.
    (a put List('x', 'y', 'z'), a.pos) === ("zyxXYZ", List('a', 'b', 'c', 'd'))
  }

  // Reduction on the state data structure.
  // The state data type represents a function accepting a state value
  // and returning the pair of values:
  //   * a new state value
  //   * an arbitrary value
  "state reduction" >> {
    // Produce a reduction on a state value where the state value is of the type Int
    // and the arbitrary computed value is of the type String.
    // The string reduction is used through reduction on the state data structure.
    // State[Int, String] can be thought of as a function: Int => (Int, String).
    val r: Reduction[State[Int, String]] =
      string.state
    // Reduce two state values.
    // The first state value modifies the Int state by adding 10 and produces an arbitrary value
    // that appends the Int state to a string ("abc").
    // The second state value modifies the Int state by multiplying by 2 and produces
    // an arbitrary value that is the string "def".
    val a: State[Int, String] =
      r(State(n => (n + 10, "abc" + n)), State(n => (n * 2, "def")))
    a(11) === (42, "abc11def")
  }

  // Reduction on the java.lang.Comparable interface.
  // Reduction occurs by looking for the first non-zero (representing inequality) value
  // or if none is found, then resulting in equality.
  "comparable reduction" >> {
    val r: Reduction[Comparable[Int]] =
      comparable
    // Reduce the pair of values 7 then 8. The will result in inequality (less-than).
    val a: Comparable[Int] =
      r(7, 8)
    // Assert the result of 4 applications of the comparable.
    // * To the value 6 resulting in less-than.
    // * To the value 7 resulting in less-than.
    // * To the value 8 resulting in greater-than.
    // * To the value 9 resulting in greater-than.
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
