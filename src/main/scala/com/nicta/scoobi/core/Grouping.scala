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

import java.lang.Comparable
import annotation.implicitNotFound


/** Specify the way in which key-values are "shuffled". Used by 'groupByKey' in
 * 'DList'. */
@implicitNotFound(msg = "Cannot find Grouping type class for ${K}")
trait Grouping[K] {
  /** Specifies how key-values are partitioned among Reducer tasks. */
  def partition(key: K, num: Int): Int = (key.hashCode & Int.MaxValue) % num

  /** Specifies the order in which grouped values are presented to a Reducer
   * task, for a given partition. */
  def sortCompare(x: K, y: K): Int = groupCompare(x, y)

  /** Specifies how values, for a given partition, are grouped together in
   * a given partition. */
  def groupCompare(x: K, y: K): Int
}

object Grouping extends GroupingImplicits

/** Implicit definitions of Grouping instances for common types. */
trait GroupingImplicits {

  /** An implicitly Grouping type class instance where sorting is implemented via an Ordering
   * type class instance. Partitioning and grouping use the default implementation. */
  implicit def OrderingGrouping[T : Ordering] = new Grouping[T] {
    def groupCompare(x: T, y: T): Int = implicitly[Ordering[T]].compare(x, y)
  }

  /** An implicitly Grouping type class instance where sorting is implemented via an Ordering
   * type class instance. Partitioning and grouping use the default implementation. */
  implicit def ComparableGrouping[T <: Comparable[T]] = new Grouping[T] {
    def groupCompare(x: T, y: T): Int = x.compareTo(y)
  }

  def mkCaseOrdering[T, A1: Ordering](apply: (A1) => T, unapply: T => Option[(A1)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first, second)
      if (c1 != 0) return c1
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering](apply: (A1, A2) => T, unapply: T => Option[(A1, A2)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering](apply: (A1, A2, A3) => T, unapply: T => Option[(A1, A2, A3)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering, A4: Ordering](apply: (A1, A2, A3, A4) => T, unapply: T => Option[(A1, A2, A3, A4)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      val c4 = implicitly[Ordering[A4]].compare(first._4, second._4)
      if (c4 != 0) return c4
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering, A4: Ordering, A5: Ordering](apply: (A1, A2, A3, A4, A5) => T, unapply: T => Option[(A1, A2, A3, A4, A5)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      val c4 = implicitly[Ordering[A4]].compare(first._4, second._4)
      if (c4 != 0) return c4
      val c5 = implicitly[Ordering[A5]].compare(first._5, second._5)
      if (c5 != 0) return c5
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering, A4: Ordering, A5: Ordering, A6: Ordering](apply: (A1, A2, A3, A4, A5, A6) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      val c4 = implicitly[Ordering[A4]].compare(first._4, second._4)
      if (c4 != 0) return c4
      val c5 = implicitly[Ordering[A5]].compare(first._5, second._5)
      if (c5 != 0) return c5
      val c6 = implicitly[Ordering[A6]].compare(first._6, second._6)
      if (c6 != 0) return c6
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering, A4: Ordering, A5: Ordering, A6: Ordering, A7: Ordering](apply: (A1, A2, A3, A4, A5, A6, A7) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      val c4 = implicitly[Ordering[A4]].compare(first._4, second._4)
      if (c4 != 0) return c4
      val c5 = implicitly[Ordering[A5]].compare(first._5, second._5)
      if (c5 != 0) return c5
      val c6 = implicitly[Ordering[A6]].compare(first._6, second._6)
      if (c6 != 0) return c6
      val c7 = implicitly[Ordering[A7]].compare(first._7, second._7)
      if (c7 != 0) return c7
      0
    }
  }

  def mkCaseOrdering[T, A1: Ordering, A2: Ordering, A3: Ordering, A4: Ordering, A5: Ordering, A6: Ordering, A7: Ordering, A8: Ordering](apply: (A1, A2, A3, A4, A5, A6, A7, A8) => T, unapply: T => Option[(A1, A2, A3, A4, A5, A6, A7, A8)]): Ordering[T] = new Ordering[T] {
    def compare(x: T, y: T): Int = {
      val first = unapply(x).get
      val second = unapply(y).get

      val c1 = implicitly[Ordering[A1]].compare(first._1, second._1)
      if (c1 != 0) return c1
      val c2 = implicitly[Ordering[A2]].compare(first._2, second._2)
      if (c2 != 0) return c2
      val c3 = implicitly[Ordering[A3]].compare(first._3, second._3)
      if (c3 != 0) return c3
      val c4 = implicitly[Ordering[A4]].compare(first._4, second._4)
      if (c4 != 0) return c4
      val c5 = implicitly[Ordering[A5]].compare(first._5, second._5)
      if (c5 != 0) return c5
      val c6 = implicitly[Ordering[A6]].compare(first._6, second._6)
      if (c6 != 0) return c6
      val c7 = implicitly[Ordering[A7]].compare(first._7, second._7)
      if (c7 != 0) return c7
      val c8 = implicitly[Ordering[A8]].compare(first._8, second._8)
      if (c8 != 0) return c8
      0
    }
  }
}
