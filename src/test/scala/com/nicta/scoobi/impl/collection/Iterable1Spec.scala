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
package impl
package collection

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.specs2.ScalaCheck
import testing.mutable.UnitSpecification

object Iterable1Data {
  import Iterable1._

  implicit def ArbitraryIterable1[A: Arbitrary]: Arbitrary[Iterable1[A]] =
    Arbitrary(for {
      h <- arbitrary[A]
      t <- arbitrary[List[A]]
    } yield h +:: t)
}

class Iterable1Spec extends UnitSpecification with ScalaCheck {
  import Iterable1._
  import Iterable1Data._

  "flatten then size is sum of sizes" >> prop {
    i: Iterable1[Iterable1[Int]] =>
      i.flatten.size == i.map(_.size).reduceLeft(_+_)
  }

  "flatten two is same as ++" >> prop {
    (i: Iterable1[Int], j: Iterable1[Int]) =>
      (i +:: List(j)).flatten == (i ++ j)
  }

  "map satisfies identity" >> prop {
    (i: Iterable1[Int]) =>
      (i map identity) == i
  }

  "map satisfies composition" >> prop {
    (i: Iterable1[Int], f: String => List[Char], g: Int => String) =>
      (i map g map f) == (i map (f compose g))
  }

  "flatMap satisfies associativity" >> prop {
    (i: Iterable1[Int], f: Int => Iterable1[String], g: String => Iterable1[List[Char]]) =>
      (i flatMap f flatMap g) == (i flatMap (x => f(x) flatMap g))
  }

  "filter gives elements forall" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      i filter f forall f
  }

  "partition gives elements forall and not forall" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      i partition f match {
        case LeftBreakIterable1(x, y) => f(x.head) && (x.tail forall f) && (y forall (!f(_)))
        case RightBreakIterable1(x, y) => (x forall f) && !f(y.head) && (y.tail forall (!f(_)))
      }
  }

  "forall not exists" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      (i forall f) != (i exists (!f(_)))
  }

  "exists not forall" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      (i exists f) != (i forall (!f(_)))
  }

  "count const true is size" >> prop {
    (i: Iterable1[String]) =>
      (i count (_ => true)) == i.size
  }

  "count const false is 0" >> prop {
    (i: Iterable1[String]) =>
      (i count (_ => false)) == 0
  }

  "count <= size" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      (i count f) <= i.size
  }

  "find is when exists is and forall isn't" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      i find f match {
        case None => i forall (!f(_))
        case Some(a) => f(a) && (i exists f)
      }
  }

  "take gives maximum size" >> prop {
    (i: Iterable1[String], n: Int) =>
      (i take n).size <= math.max(0, n)
  }

  "takeWhile gives elements forall" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      i takeWhile f forall f
  }

  "dropWhile gives not next" >> prop {
    (i: Iterable1[String], f: String => Boolean) => {
      val j = (i dropWhile f).iterator
      !(j.hasNext && f(j.next))
    }
  }

  "span gives elements forall and first not forall" >> prop {
    (i: Iterable1[String], f: String => Boolean) =>
      i span f match {
        case LeftBreakIterable1(x, y) => f(x.head) && (x.tail forall f) && {
          val i = y.iterator
          (!i.hasNext || f(i.next))
        }
        case RightBreakIterable1(x, y) => {
          val i = x.iterator
          (!i.hasNext || f(i.next))
        } && !f(y.head) && (y.tail forall (!f(_)))
      }
  }


}
