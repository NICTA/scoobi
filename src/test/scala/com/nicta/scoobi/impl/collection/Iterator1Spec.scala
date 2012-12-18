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

object Iterator1Data {
  import Iterator1._

  implicit def ArbitraryIterator1[A: Arbitrary]: Arbitrary[Iterator1[A]] =
    Arbitrary(for {
      h <- arbitrary[A]
      t <- arbitrary[List[A]]
      n <- org.scalacheck.Gen.choose(0, t.length + 1)
    } yield {
      val i = h +:: t.iterator
      0 until n foreach (_ => i.next)
      i
    })

}

// It may appear that some of these tests are incomplete and/or without coverage.
// This is true, but it is an inevitable consequence of side-effecting code.
// Running some Iterator functions stomps the previous state, so that it cannot be
// compared against some other computation.
//
// Do not be tempted to apply equational reasoning here.
class Iterator1Spec extends UnitSpecification with ScalaCheck {
  import Iterator1._
  import Iterator1Data._

  "hasNext gives next" >> prop {
    i: Iterator1[Int] =>
      i.next must throwA[NoSuchElementException].iff(!i.hasNext)
  }

  "seq has same hasNext" >> prop {
    i: Iterator1[Int] =>
      i.hasNext == i.seq.hasNext
  }


  "toTraversable produces same head" >> prop {
    i: Iterator1[Int] =>
      i.toTraversable.head == i.first
  }

  "toIterator has same hasNext" >> prop {
    i: Iterator1[Int] =>
      i.hasNext == i.toIterator.hasNext
  }

  "isEmpty gives no next" >> prop {
    i: Iterator1[Int] =>
      i.next must throwA[NoSuchElementException].iff(i.isEmpty)
  }

  "take gives maximum size" >> prop {
    (i: Iterator1[String], n: Int) =>
      (i take n).size <= math.max(0, n)
  }

  "first maps" >> prop {
    (i: Iterator1[String], f: String => Int) =>
      (i map f).first == f(i.first)
  }

  "forall meets first" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      !(i forall f) || f(i.first)
  }

  "filter gives elements forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i filter f forall f
  }

  "withFilter gives elements forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i withFilter f forall f
  }

  "filterNot gives elements not forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i filterNot f forall (!f(_))
  }

  "takeWhile gives elements forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i takeWhile f forall f
  }

  "partition gives elements forall and not forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i partition f match {
        case LeftBreakIterator1(x, y) => f(x.first) && (x.rest forall f) && (y forall (!f(_)))
        case RightBreakIterator1(x, y) => (x forall f) && !f(y.first) && (y.rest forall (!f(_)))
      }
  }

  "span gives elements forall and first not forall" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      i span f match {
        case LeftBreakIterator1(x, y) => f(x.first) && (x.rest forall f) && (!y.hasNext || f(y.next))
        case RightBreakIterator1(x, y) => (!x.hasNext || f(x.next)) && !f(y.first) && (y.rest forall (!f(_)))
      }
  }

  "dropWhile gives not next" >> prop {
    (i: Iterator1[String], f: String => Boolean) => {
      val j = i dropWhile f
      !(j.hasNext && f(j.next))
    }
  }

  "exists meets first" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      (i exists f) || !f(i.first)
  }

  "contains meets first" >> prop {
    (i: Iterator1[String], s: String) =>
      (i contains s) || i.first != s
  }

  "find meets first" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      (i find f exists f) || !f(i.first)
  }

  "indexWhere meets first" >> prop {
    (i: Iterator1[String], f: String => Boolean) =>
      (i.indexWhere(f) != -1) || !f(i.first)
  }

  "indexOf meets first" >> prop {
    (i: Iterator1[String], s: String) =>
      (i.indexOf(s) != -1) || i.first != s
  }

  "length >= 1" >> prop {
    (i: Iterator1[String]) =>
      i.length >= 1
  }


}
