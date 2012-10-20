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
package lib

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ArrayBuffer
import scala.Right
import scala.Left
import core._

case class Relational[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat](
  left: DList[(K, A)]) {
  /** Perform an equijoin with another distributed lists. */
  def join[B: Manifest: WireFormat](right: DList[(K, B)]): DList[(K, (A, B))] = Relational.join(left, right)

  /**
   * Perform a right outer-join of two (2) distributed lists. Note the return type of Option[A]
   * as when there is no value in this dlist for a value on the right dlist, it will
   * return none.
   */
  def joinRight[B: Manifest: WireFormat](right: DList[(K, B)]): DList[(K, (Option[A], B))] = Relational.joinRight(left, right)

  /**
   * Perform a left outer-join of two (2) distributed lists. Note the return type of Option[A]
   * as when there is no value in this dlist for a value on the right dlist, it will
   * return none.
   */
  def joinLeft[B: Manifest: WireFormat](right: DList[(K, B)]): DList[(K, (A, Option[B]))] = Relational.joinLeft(left, right)

  /**
   * Perform a full outer-join of two distributed lists. The default function specifies how
   * to construct a A or B when there is none. Note at least one of the A or B should exist
   */
  def joinFullOuter[B: Manifest: WireFormat, V: Manifest: WireFormat](
    right: DList[(K, B)],
    hasLeft: (K, A) => V,
    hasRight: (K, B) => V,
    hasBoth: (K, A, B) => V): DList[(K, V)] = Relational.joinFullOuter(left, right, hasLeft, hasRight, hasBoth)

  /**
   * Perform a full outer-join of two distributed lists. Note how it returns an Option[A] and Option[B], but it
   * shouldn't be possible for both to be None.
   */
  def joinFullOuter[B: Manifest: WireFormat](
    right: DList[(K, B)]): DList[(K, (Option[A], Option[B]))] = Relational.joinFullOuter(left, right)

  /** Perform a co-group with another distributed lists */
  def coGroup[B: Manifest: WireFormat](right: DList[(K, B)]): DList[(K, (Iterable[A], Iterable[B]))] = Relational.coGroup(left, right)

}

object Relational {

  /** Perform an equijoin of two (2) distributed lists. */
  def join[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat](
      left: DList[(K, A)],
      right: DList[(K, B)]): DList[(K, (A, B))] = joinWith(left, right)(innerJoin)

  /**
   * Perform a right outer-join of two (2) distributed lists. Note the return type of Option[A]
   * as when there is no value in the left dlist (d1) for a value on the right dlist (d2), it will
   * return none.
   */
  def joinRight[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat](
      left: DList[(K, A)],
      right: DList[(K, B)]): DList[(K, (Option[A], B))] = joinWith(left, right)(rightOuterJoin)

  /**
   * Perform a left outer-join of two (2) distributed lists. Note the return type of Option[B]
   * for when there is no value in the right dlist (d1).
   */
  def joinLeft[K: Manifest: WireFormat: Grouping,
    A: Manifest: WireFormat,
    B: Manifest: WireFormat](
        left: DList[(K, A)],
        right: DList[(K, B)]): DList[(K, (A, Option[B]))] = joinRight(right, left).map(v => (v._1, v._2.swap))

  /**
   * Perform a full outer-join of two distributed lists. The default function specifies how
   * to construct a A or B when there is none
   */
  def joinFullOuter[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat, V: Manifest: WireFormat](
    l: DList[(K, A)],
    r: DList[(K, B)],
    hasLeft: (K, A) => V,
    hasRight: (K, B) => V,
    hasBoth: (K, A, B) => V): DList[(K, V)] = joinWith(l, r)(fullOuterJoin[K, A, B, V](hasLeft, hasRight, hasBoth))

  /**
   * Perform a full outer-join of two distributed lists. The default function specifies how
   * to construct a A or B when there is none
   */
  def joinFullOuter[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat](
    l: DList[(K, A)],
    r: DList[(K, B)]): DList[(K, (Option[A], Option[B]))] = joinFullOuter(l, r,
    (k: K, a: A) => (Some(a), None),
    (k: K, b: B) => (None, Some(b)),
    (k: K, a: A, b: B) => (Some(a), Some(b)))

  /**
   * Perform a left outer-join of two (2) distributed lists. Note the return type of Option[B]
   * for when there is no value in the right dlist (d1).
   */
  def outerJoin[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat](d1: DList[(K, A)], d2: DList[(K, B)]): DList[(K, (A, Option[B]))] = joinRight(d2, d1).map(v => (v._1, v._2.swap))

  /** Perform a co-group of two (2) distributed lists */
  def coGroup[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat](d1: DList[(K, A)], d2: DList[(K, B)]): DList[(K, (Iterable[A], Iterable[B]))] = {
    val d1s: DList[(K, Either[A, B])] = d1 map { case (k, a1) => (k, Left(a1)) }
    val d2s: DList[(K, Either[A, B])] = d2 map { case (k, a2) => (k, Right(a2)) }

    val grouped = (d1s ++ d2s).groupByKey map {
      case (k, as) => {
        val vb1 = new VectorBuilder[A]()
        val vb2 = new VectorBuilder[B]()
        as foreach {
          case Left(a1) => vb1 += a1
          case Right(a2) => vb2 += a2
        }
        (k, (vb1.result().toIterable, vb2.result().toIterable))
      }
    }

    grouped.groupBarrier
  }

  private def innerJoin[T, A, B] = new BasicDoFn[((T, Boolean), Iterable[Either[A, B]]), (T, (A, B))] {
    def process(input: ((T, Boolean), Iterable[Either[A, B]]), emitter: Emitter[(T, (A, B))]) {
      var alist = new ArrayBuffer[A]

      for (v <- input._2) {
        v match {
          case Left(a) => alist += a
          case Right(b) => for (a <- alist) emitter.emit((input._1._1, (a, b)))
        }
      }
    }
  }

  private def rightOuterJoin[T, A, B] = new BasicDoFn[((T, Boolean), Iterable[Either[A, B]]), (T, (Option[A], B))] {
    def process(input: ((T, Boolean), Iterable[Either[A, B]]), emitter: Emitter[(T, (Option[A], B))]) {
      var alist = new ArrayBuffer[A]

      for (v <- input._2) {
        v match {
          case Left(a) => alist += a
          case Right(b) => {
            if (alist.isEmpty)
              emitter.emit((input._1._1, (None, b)))
            else
              for (a <- alist) emitter.emit((input._1._1, (Some(a), b)))
          }
        }
      }
    }
  }

  private def fullOuterJoin[T, A, B, V](
    hasLeft: (T, A) => V,
    hasRight: (T, B) => V,
    hasBoth: (T, A, B) => V): BasicDoFn[((T, Boolean), Iterable[Either[A, B]]), (T, V)] = new BasicDoFn[((T, Boolean), Iterable[Either[A, B]]), (T, V)] {
    def process(input: ((T, Boolean), Iterable[Either[A, B]]), emitter: Emitter[(T, V)]) {
      val alist = new ArrayBuffer[A]
      var bseen = false
      val key = input._1._1

      for (v <- input._2) {
        v match {
          case Left(a) => alist += a
          case Right(b) => {
            bseen = true
            if (alist.isEmpty)
              emitter.emit((key, hasRight(key, b)))
            else
              for (a <- alist) {
                emitter.emit((key, hasBoth(key, a, b)))
              }
          }
        }
      }

      if (!bseen)
        for (a <- alist) {
          emitter.emit((key, hasLeft(key, a)))
        }
    }
  }

  /** Perform a join of two distributed lists using a specified join-predicate, and a type. */
  private def joinWith[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat, B: Manifest: WireFormat, V: Manifest: WireFormat](
    d1: DList[(K, A)],
    d2: DList[(K, B)])(dofn: BasicDoFn[((K, Boolean), Iterable[Either[A, B]]), (K, V)]): DList[(K, V)] = {

    /* Map left and right DLists to be of the same type. Label the left as 'true' and the
     * right as 'false'. Note the hack cause DList doesn't yet have the co/contravariance. */
    val left = d1.map(v => { val e: Either[A, B] = Left[A, B](v._2); ((v._1, true), e) })
    val right = d2.map(v => { val e: Either[A, B] = Right[A, B](v._2); ((v._1, false), e) })

    /* Grouping type class instance that implements a secondary sort to ensure left
     * values come before right values. */
    implicit val grouping = new Grouping[(K, Boolean)] {
      override def partition(key: (K, Boolean), num: Int): Int =
        implicitly[Grouping[K]].partition(key._1, num)

      override def groupCompare(a: (K, Boolean), b: (K, Boolean)): Int =
        implicitly[Grouping[K]].groupCompare(a._1, b._1)

      override def sortCompare(a: (K, Boolean), b: (K, Boolean)): Int = {
        val n = implicitly[Grouping[K]].sortCompare(a._1, b._1)
        if (n != 0)
          n
        else (a._2, b._2) match {
          case (true, false) => -1
          case (false, true) => 1
          case _ => n
        }
      }
    }

    (left ++ right).groupByKey.parallelDo(dofn).groupBarrier
  }

}

