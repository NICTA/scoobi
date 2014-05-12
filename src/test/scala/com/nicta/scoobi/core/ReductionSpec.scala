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

import org.specs2.ScalaCheck
import com.nicta.scoobi.testing.mutable.UnitSpecification
import Reduction.{Reduction => R}

import scalaz.{scalacheck, Endo, Digit, std, NonEmptyList, Ordering}
import std.anyVal._
import std.math.bigInt._
import std.stream._
import std.option._
import std.list._
import std.string._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._

//import scalacheck.ScalazArbitrary._

class ReductionSpec extends UnitSpecification with ScalaCheck {
  import Reduction._

  "construct => reduce" >> prop {
      (f: (Int, Int) => Int, x: Int, y: Int) =>
        Reduction(f).reduce(x, y) == f(x, y)
    }

  "constant" >> prop {
      (f: Int, x: Int, y: Int) =>
        R.constant(f).reduce(x, y) == f
    }

  "split1" >> prop {
      (f: Int => Int, x: Int, y: Int) =>
        R.split1(f).reduce(x, y) == f(x)
    }

  "split2" >> prop {
      (f: Int => Int, x: Int, y: Int) =>
        R.split2(f).reduce(x, y) == f(y)
    }

  "first" >> prop {
      (x: Int, y: Int) =>
        R.first[Int].reduce(x, y) == x
    }

  "last" >> prop {
      (x: Int, y: Int) =>
        R.last[Int].reduce(x, y) == y
    }

  "first is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.first[Int].associative(x, y, z)
    }

  "last is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.last[Int].associative(x, y, z)
    }

  "firstOption is associative" >> prop {
      (x: Option[Int], y: Option[Int], z: Option[Int]) =>
        R.firstOption[Int].associative(x, y, z)
    }

  "lastOption is associative" >> prop {
      (x: Option[Int], y: Option[Int], z: Option[Int]) =>
        R.lastOption[Int].associative(x, y, z)
    }

  "minimum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.minimum[Int].associative(x, y, z)
    }

  "maximum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.maximum[Int].associative(x, y, z)
    }

  "endo is associative" >> prop {
      (x: Int => Int, y: Int => Int, z: Int => Int, a: Int) =>
        R.endo[Int].associative.by(x, y, z)(_(a))
    }

  "endoE is associative" >> prop {
      (x: Int => Int, y: Int => Int, z: Int => Int, a: Int) =>
        R.endoE[Int].associative.as(x, y, z)(Endo(_))((x, y) => x(a) == y(a))
    }

  "or is associative" >> prop {
      (x: Boolean, y: Boolean, z: Boolean) =>
        R.or.associative(x, y, z)
    }

  "and is associative" >> prop {
      (x: Boolean, y: Boolean, z: Boolean) =>
        R.and.associative(x, y, z)
    }

  "ordering is associative" >> prop {
      (x: Ordering, y: Ordering, z: Ordering) =>
        R.ordering.associative(x, y, z)
    }

  "string is associative" >> prop {
      (x: String, y: String, z: String) =>
        R.string.associative(x, y, z)
    }

  "list is associative" >> prop {
      (x: List[Int], y: List[Int], z: List[Int]) =>
        R.list[Int].associative(x, y, z)
    }

  "stream is associative" >> prop {
      (x: Stream[Int], y: Stream[Int], z: Stream[Int]) =>
        R.stream[Int].associative(x, y, z)
    }

  "non-empty list is associative" >> prop {
      (x: NonEmptyList[Int], y: NonEmptyList[Int], z: NonEmptyList[Int]) =>
        R.nonEmptyList[Int].associative(x, y, z)
    }

  "big int sum is associative" >> prop {
      (x: BigInt, y: BigInt, z: BigInt) =>
        R.Sum.bigint.associative(x, y, z)
    }

  "byte sum is associative" >> prop {
      (x: Byte, y: Byte, z: Byte) =>
        R.Sum.byte.associative(x, y, z)
    }

  "char sum is associative" >> prop {
      (x: Char, y: Char, z: Char) =>
        R.Sum.char.associative(x, y, z)
    }

  "int sum is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.Sum.int.associative(x, y, z)
    }

  "long sum is associative" >> prop {
      (x: Long, y: Long, z: Long) =>
        R.Sum.long.associative(x, y, z)
    }

  "short sum is associative" >> prop {
      (x: Short, y: Short, z: Short) =>
        R.Sum.short.associative(x, y, z)
    }

  "big int product is associative" >> prop {
      (x: BigInt, y: BigInt, z: BigInt) =>
        R.Product.bigint.associative(x, y, z)
    }

  "byte product is associative" >> prop {
      (x: Byte, y: Byte, z: Byte) =>
        R.Product.byte.associative(x, y, z)
    }

  "char product is associative" >> prop {
      (x: Char, y: Char, z: Char) =>
        R.Product.char.associative(x, y, z)
    }

  "int product is associative" >> prop {
      (x: Int, y: Int, z: Int) =>
        R.Product.int.associative(x, y, z)
    }

  "long product is associative" >> prop {
      (x: Long, y: Long, z: Long) =>
        R.Product.long.associative(x, y, z)
    }

  "short product is associative" >> prop {
      (x: Short, y: Short, z: Short) =>
        R.Product.short.associative(x, y, z)
    }

  import NonEmptyList._
  implicit def NonEmptyListArbitrary[A : Arbitrary]: Arbitrary[NonEmptyList[A]] = Arbitrary(for {
    a  <- implicitly[Arbitrary[A]].arbitrary
    la <- implicitly[Arbitrary[List[A]]].arbitrary
  } yield (nel(a, la)))

  import scalaz.Ordering._
  implicit def OrderingArbitrary: Arbitrary[Ordering] = Arbitrary(org.scalacheck.Gen.oneOf(LT, EQ, GT))

}
