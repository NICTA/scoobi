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
import scalaz.{Ordering => SOrdering, _}, Scalaz._
import scalaz.Ordering.{LT, EQ, GT}

/**
  * Specify the way in which key-values are "shuffled". Used by `groupByKey` in `DList`
 */
@implicitNotFound(msg = "Cannot find Grouping type class for ${K}")
trait Grouping[K] extends KeyGrouping {
  private[scoobi] def partitionKey(k: Any, num: Int) = partition(k.asInstanceOf[K], num)
  private[scoobi] def sortKey(k1: Any, k2: Any)      = sortCompare(k1.asInstanceOf[K], k2.asInstanceOf[K])
  private[scoobi] def groupKey(k1: Any, k2: Any)     = groupCompare(k1.asInstanceOf[K], k2.asInstanceOf[K])

  /** Specifies how key-values are partitioned among Reducer tasks. */
  def partition(key: K, num: Int): Int = (key.hashCode & Int.MaxValue) % num

  /** Specifies the order in which grouped values are presented to a Reducer
   * task, for a given partition. */
  def sortCompare(x: K, y: K): SOrdering = groupCompare(x, y)

  /** Specifies how values, for a given partition, are grouped together in
   * a given partition. */
  def groupCompare(x: K, y: K): SOrdering

  /** Interface to `scalaz.Order` on `sortCompare` */
  def sortOrder: Order[K] =
    Order.order(sortCompare(_, _))

  /** Interface to `scalaz.Order` on `groupCompare` */
  def groupOrder: Order[K] =
    Order.order(groupCompare(_, _))

  /** Interface to `scala.Ordering` on `sortCompare` */
  def sortOrdering: Ordering[K] =
    new Ordering[K] {
      def compare(x: K, y: K): Int = sortCompare(x, y).toInt
    }

  /** Interface to `scala.Ordering` on `groupCompare` */
  def groupOrdering: Ordering[K] =
    new Ordering[K] {
      def compare(x: K, y: K): Int = groupCompare(x, y).toInt
    }

  /** Does sort compare produce equal elements? */
  def isSortEqual(k1: K, k2: K): Boolean =
    sortCompare(k1, k2) == EQ

  /** Does group compare produce equal elements? */
  def isGroupEqual(k1: K, k2: K): Boolean =
    groupCompare(k1, k2) == EQ

  /** Does sort compare produce the first less than the second? */
  def isSortLessThan(k1: K, k2: K): Boolean =
    sortCompare(k1, k2) == LT

  /** Does group compare produce the first less than the second? */
  def isGroupLessThan(k1: K, k2: K): Boolean =
    groupCompare(k1, k2) == LT

  /** Does sort compare produce the first greater than the second? */
  def isSortGreaterThan(k1: K, k2: K): Boolean =
    sortCompare(k1, k2) == GT

  /** Does group compare produce the first greater than the second? */
  def isGroupGreaterThan(k1: K, k2: K): Boolean =
    groupCompare(k1, k2) == GT

  /** Map on this grouping contravariantly. */
  def contramap[L](f: L => K): Grouping[L] =
    new Grouping[L] {
      override def partition(key: L, num: Int): Int =
        Grouping.this.partition(f(key), num)
      override def sortCompare(x: L, y: L): SOrdering =
        Grouping.this.sortCompare(f(x), f(y))
      def groupCompare(x: L, y: L): SOrdering =
        Grouping.this.groupCompare(f(x), f(y))
    }

  /** Combine two groupings to a grouping of product type. Alias for `***` */
  def zip[L](q: Grouping[L]): Grouping[(K, L)] =
    new Grouping[(K, L)] {
      override def partition(key: (K, L), num: Int): Int =
        q.partition(key._2, Grouping.this.partition(key._1, num))
      override def sortCompare(x: (K, L), y: (K, L)): SOrdering =
        Grouping.this.sortCompare(x._1, y._1) |+| q.sortCompare(x._2, y._2)
      def groupCompare(x: (K, L), y: (K, L)): SOrdering =
        Grouping.this.groupCompare(x._1, y._1) |+| q.groupCompare(x._2, y._2)
    }

  /** Combine two groupings to a grouping of product type. Alias for `zip` */
  def ***[L](q: Grouping[L]): Grouping[(K, L)] =
    zip(q)

  /** Add two groupings together. */
  def |+|(q: Grouping[K]): Grouping[K] =
    new Grouping[K] {
      override def partition(key: K, num: Int): Int =
        q.partition(key, Grouping.this.partition(key, num))
      override def sortCompare(x: K, y: K): SOrdering =
        Grouping.this.sortCompare(x, y) |+| q.sortCompare(x, y)
      def groupCompare(x: K, y: K): SOrdering =
        Grouping.this.groupCompare(x, y) |+| q.groupCompare(x, y)
    }

  /** Construct grouping for secondary sort. */
  def secondarySort[L](q: Grouping[L]): Grouping[(K, L)] =
    new Grouping[(K, L)] {
      override def partition(key: (K, L), num: Int): Int =
        Grouping.this.partition(key._1, num)
      override def sortCompare(x: (K, L), y: (K, L)): SOrdering =
        groupCompare(x, y) |+| q.groupCompare(x._2, y._2)
      def groupCompare(x: (K, L), y: (K, L)): SOrdering =
        Grouping.this.groupCompare(x._1, y._1)
    }


}

object Grouping extends GroupingImplicits with GroupingFunctions {
  def all[K] = new AllGrouping
  class AllGrouping[K] extends Grouping[K] {
    def groupCompare(x: K, y: K) = EQ
    override def toString = "AllGrouping"
  }
}

trait GroupingFunctions {
  /** Partition a grouping of sums into a product of groupings. */
  def partition[A, B](q: Grouping[A \/ B]): (Grouping[A], Grouping[B]) =
    (
      new Grouping[A] {
        override def partition(key: A, num: Int): Int =
          q.partition(key.left, num)
        override def sortCompare(x: A, y: A): SOrdering =
          q.sortCompare(x.left, y.left)
        def groupCompare(x: A, y: A): SOrdering =
          q.groupCompare(x.left, y.left)
      }
    , new Grouping[B] {
        override def partition(key: B, num: Int): Int =
          q.partition(key.right, num)
        override def sortCompare(x: B, y: B): SOrdering =
          q.sortCompare(x.right, y.right)
        def groupCompare(x: B, y: B): SOrdering =
          q.groupCompare(x.right, y.right)
      }
    )

  /** The identity grouping. */
  def groupingId[K]: Grouping[K] =
    new Grouping[K] {
      override def partition(key: K, num: Int): Int =
        num
      override def sortCompare(x: K, y: K): SOrdering =
        Monoid[SOrdering].zero
      def groupCompare(x: K, y: K): SOrdering =
        Monoid[SOrdering].zero
    }
}

/** Implicit definitions of Grouping instances for common types. */
trait GroupingImplicits extends GroupingImplicits0 {

  /** An implicitly Grouping type class instance where sorting is implemented via an Ordering
   * type class instance. Partitioning and grouping use the default implementation. */
  implicit def OrderGrouping2[T : scalaz.Order]: Grouping[T] = new Grouping[T] {
    override def toString = "OrderingGrouping"
    def groupCompare(x: T, y: T) =
      implicitly[Order[T]].order(x, y)
  }

  /** An implicitly Grouping type class instance where sorting is implemented via an Ordering
   * type class instance. Partitioning and grouping use the default implementation. */
  implicit def ComparableGrouping[T <: Comparable[T]]: Grouping[T] = new Grouping[T] {
    override def toString = "ComparableGrouping"
    def groupCompare(x: T, y: T) = SOrdering.fromInt(x.compareTo(y))
  }

  /** Instances for Shapeless tagged types. */
  import shapeless.TypeOperators._

  implicit def taggedTypeGrouping[T : Grouping, U]: Grouping[T @@ U] =
    implicitly[Grouping[T]].asInstanceOf[Grouping[T @@ U]]

  implicit def taggedTypeOrdering[T : Ordering, U]: Ordering[T @@ U] =
    implicitly[Ordering[T]].asInstanceOf[Ordering[T @@ U]]

}

trait GroupingImplicits0 {

  /** An implicitly Grouping type class instance where sorting is implemented via an Ordering
   * type class instance. Partitioning and grouping use the default implementation. */
  implicit def OrderingGrouping[T : Ordering]: Grouping[T] = new Grouping[T] {
      override def toString = "OrderingGrouping"
    def groupCompare(x: T, y: T) =
      SOrdering.fromInt(implicitly[Ordering[T]].compare(x, y))
  }
}

import scalaz.{Ordering => SOrdering}
/**
 * Internal untyped grouping typeclass
 */
private[scoobi]
trait KeyGrouping {
  private[scoobi] def partitionKey(k: Any, num: Int): Int
  private[scoobi] def sortKey(k1: Any, k2: Any): SOrdering
  private[scoobi] def groupKey(k1: Any, k2: Any): SOrdering

  private[scoobi] def isEqualWithSort(k1: Any, k2: Any): Boolean  = sortKey(k1, k2) == EQ
  private[scoobi] def isEqualWithGroup(k1: Any, k2: Any): Boolean = groupKey(k1, k2) == EQ
  private[scoobi] def toSortOrder: Order[Any] = Order.order(sortKey(_, _))
  private[scoobi] def toGroupOrder: Order[Any] = Order.order(groupKey(_, _))
  private[scoobi] def toSortOrdering: Ordering[Any]  = new Ordering[Any] { def compare(x: Any, y: Any): Int = sortKey(x, y).toInt }
  private[scoobi] def toGroupOrdering: Ordering[Any] = new Ordering[Any] { def compare(x: Any, y: Any): Int = groupKey(x, y).toInt  }
}

