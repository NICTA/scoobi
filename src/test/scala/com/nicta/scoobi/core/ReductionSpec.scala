package com.nicta.scoobi
package core

import org.specs2.ScalaCheck
import com.nicta.scoobi.testing.mutable.UnitSpecification

import scalaz._, Scalaz._, scalacheck.ScalazArbitrary._

class ReductionSpec extends UnitSpecification with ScalaCheck {
  import Reduction._

  "construct => reduce" >> prop {
      (f: (Int, Int) => Int, x: Int, y: Int) =>
        Reduction(f).reduce(x, y) == f(x, y)
    }

  "constant" >> prop {
      (f: Int, x: Int, y: Int) =>
        constant(f).reduce(x, y) == f
    }

  "split1" >> prop {
      (f: Int => Int, x: Int, y: Int) =>
        split1(f).reduce(x, y) == f(x)
    }

  "split2" >> prop {
      (f: Int => Int, x: Int, y: Int) =>
        split2(f).reduce(x, y) == f(y)
    }

  "first" >> prop {
      (x: Int, y: Int) =>
        first[Int].reduce(x, y) == x
    }

  "last" >> prop {
      (x: Int, y: Int) =>
        last[Int].reduce(x, y) == y
    }

  "first is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        first[Int].associative(x, y, z)
    }

  "last is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        last[Int].associative(x, y, z)
    }

  "firstOption is associative" >> prop {
      (x: Option[Int], y: Option[Int], z: Option[Int]) =>
        firstOption[Int].associative(x, y, z)
    }

  "lastOption is associative" >> prop {
      (x: Option[Int], y: Option[Int], z: Option[Int]) =>
        lastOption[Int].associative(x, y, z)
    }

  "minimum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        minimum[Int].associative(x, y, z)
    }

  "maximum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        maximum[Int].associative(x, y, z)
    }

  "endo is associative" >> prop {
      (x: Int => Int, y: Int => Int, z: Int => Int, a: Int) =>
        endo[Int].associative.by(x, y, z)(_(a))
    }

  "endoE is associative" >> prop {
      (x: Int => Int, y: Int => Int, z: Int => Int, a: Int) =>
        endoE[Int].associative.as(x, y, z)(Endo(_))((x, y) => x(a) == y(a))
    }

  "or is associative" >> prop {
      (x: Boolean, y: Boolean, z: Boolean) =>
        or.associative(x, y, z)
    }

  "and is associative" >> prop {
      (x: Boolean, y: Boolean, z: Boolean) =>
        and.associative(x, y, z)
    }

  "ordering is associative" >> prop {
      (x: Ordering, y: Ordering, z: Ordering) =>
        ordering.associative(x, y, z)
    }

  "string is associative" >> prop {
      (x: String, y: String, z: String) =>
        string.associative(x, y, z)
    }

  "list is associative" >> prop {
      (x: List[Int], y: List[Int], z: List[Int]) =>
        list[Int].associative(x, y, z)
    }

  "stream is associative" >> prop {
      (x: Stream[Int], y: Stream[Int], z: Stream[Int]) =>
        stream[Int].associative(x, y, z)
    }

  "non-empty list is associative" >> prop {
      (x: NonEmptyList[Int], y: NonEmptyList[Int], z: NonEmptyList[Int]) =>
        nonEmptyList[Int].associative(x, y, z)
    }

  "big int sum is associative" >> prop {
      (x: BigInt, y: BigInt, z: BigInt) =>
        Sum.bigint.associative(x, y, z)
    }

  "byte sum is associative" >> prop {
      (x: Byte, y: Byte, z: Byte) =>
        Sum.byte.associative(x, y, z)
    }

  "char sum is associative" >> prop {
      (x: Char, y: Char, z: Char) =>
        Sum.char.associative(x, y, z)
    }

  "int sum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        Sum.int.associative(x, y, z)
    }

  "long sum is associative" >> prop {
      (x: Long, y: Long, z: Long) =>
        Sum.long.associative(x, y, z)
    }

  "short sum is associative" >> prop {
      (x: Short, y: Short, z: Short) =>
        Sum.short.associative(x, y, z)
    }

  "digit sum is associative" >> prop {
      (x: Digit, y: Digit, z: Digit) =>
        Sum.digit.associative(x, y, z)
    }

  "big int product is associative" >> prop {
      (x: BigInt, y: BigInt, z: BigInt) =>
        Product.bigint.associative(x, y, z)
    }

  "byte product is associative" >> prop {
      (x: Byte, y: Byte, z: Byte) =>
        Product.byte.associative(x, y, z)
    }

  "char product is associative" >> prop {
      (x: Char, y: Char, z: Char) =>
        Product.char.associative(x, y, z)
    }

  "int product is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        Product.int.associative(x, y, z)
    }

  "long product is associative" >> prop {
      (x: Long, y: Long, z: Long) =>
        Product.long.associative(x, y, z)
    }

  "short product is associative" >> prop {
      (x: Short, y: Short, z: Short) =>
        Product.short.associative(x, y, z)
    }

  "digit product is associative" >> prop {
      (x: Digit, y: Digit, z: Digit) =>
        Product.digit.associative(x, y, z)
    }

}
