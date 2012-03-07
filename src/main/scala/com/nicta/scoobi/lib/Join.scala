/**
 * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.lib

import java.io._
import scala.collection.immutable.VectorBuilder
import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.Grouping
import com.nicta.scoobi.DoFn
import com.nicta.scoobi.Emitter
import scala.collection.mutable.ArrayBuffer

object Join {

  private def innerJoin[T, A, B] = new DoFn[((T, Boolean), Iterable[Either[A, B]]), (T, (A, B))] {
    override def setup() {}
    override def process(input: ((T, Boolean), Iterable[Either[A, B]]), emitter: Emitter[(T, (A, B))]) {
      var alist = new ArrayBuffer[A]

      for (v <- input._2) {
        v match {
          case Left(a)  => alist += a
          case Right(b) => for (a <- alist) emitter.emit((input._1._1, (a, b)))
        }
      }
    }

    override def cleanup(emitter: Emitter[(T, (A, B))]) {}
  }

  private def rightOuterJoin[T, A, B, A2](has: (T, A, B) => A2, notHas: (T, B) => A2) = new DoFn[((T, Boolean), Iterable[Either[A, B]]), (T, (A2, B))] {
    override def setup() {}
    override def process(input: ((T, Boolean), Iterable[Either[A, B]]), emitter: Emitter[(T, (A2, B))]) {
      var alist = new ArrayBuffer[A]

      for (v <- input._2) {
        v match {
          case Left(a)  => alist += a
          case Right(b) => {
            if (alist.isEmpty)
              emitter.emit((input._1._1, (notHas(input._1._1, b), b)))
            else
              for (a <- alist) emitter.emit((input._1._1, (has(input._1._1, a, b), b)))
          }
        }
      }
    }

    override def cleanup(emitter: Emitter[(T, (A2, B))]) {}
  }

  /** Perform a join of two distributed lists using a specified join-predicate, and a type. */
  private def joinWith[K : Manifest : WireFormat : Grouping,
                       A : Manifest : WireFormat,
                       B : Manifest : WireFormat,
                       A2 : Manifest : WireFormat,
                       B2 : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      (dofn: DoFn[((K, Boolean), Iterable[Either[A, B]]), (K, (A2, B2))])
    : DList[(K, (A2, B2))] = {

    /* Map left and right DLists to be of the same type. Label the left as 'true' and the
     * right as 'false'. Note the hack cause DList doesn't yet have the co/contravariance. */
    val left = d1.map(v => { val e: Either[A, B] = Left[A, B](v._2); ((v._1, true), e) })
    val right = d2.map(v => { val e: Either[A, B] = Right[A, B](v._2); ((v._1, false), e) })

    /* Gropuing type class instance that implements a secondary sort to ensure left
     * values come before right values. */
    implicit val grouping = new Grouping[(K, Boolean)] {
      override def partition(key: (K, Boolean), num: Int): Int =
        implicitly[Grouping[K]].partition(key._1, num)

      override def groupCompare(a: (K, Boolean), b: (K, Boolean)): Int =
        implicitly[Grouping[K]].groupCompare(a._1, b._1)

      override def sortCompare(a: (K, Boolean), b: (K, Boolean)): Int = {
        val n = groupCompare(a, b)
        if (n != 0)
          n
        else (a._2, b._2) match {
          case (true, false) => -1
          case (false, true) => 1
          case _ => 0
        }
      }
    }

    (left ++ right).groupByKey.parallelDo(dofn).groupBarrier
  }

  /** Perform an equijoin of two (2) distributed lists. */
  def join[K : Manifest : WireFormat : Grouping,
           A : Manifest : WireFormat,
           B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
    : DList[(K, (A, B))] = joinWith(d1, d2)(innerJoin)

  /** Perform a right outer-join of two (2) distributed lists. The default function
      says how to create a value A when there was none. */
  def joinRight[K : Manifest : WireFormat : Grouping,
                A : Manifest : WireFormat,
                B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)], default: (K, B) => A)
    : DList[(K, (A, B))] = joinWith(d1, d2)(rightOuterJoin( (_, x, _) => x , default))

  /** Perform a right outer-join of two (2) distributed lists. Note the return type of Option[A]
      as when there is no value in the left dlist (d1) for a value on the right dlist (d2), it will
      return none. */
  def joinRight[K : Manifest : WireFormat : Grouping,
                A : Manifest : WireFormat,
                B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
    : DList[(K, (Option[A], B))] = joinWith(d1, d2)(rightOuterJoin( (_, a, _) => Option(a), (_, _) => None))

  /** Perform a left outer-join of two (2) distributed lists. The default function specifies how
      to construct a B, given a K and B, when there is none.*/
  def joinLeft[K : Manifest : WireFormat : Grouping,
               A : Manifest : WireFormat,
               B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)], default: (K, A) => B)
    : DList[(K, (A, B))] = joinRight(d2, d1, default).map(v => (v._1, v._2.swap))

  /** Perform a left outer-join of two (2) distributed lists. Note the return type of Option[B]
      for when there is no value in the right dlist (d1). */
  def joinLeft[K : Manifest : WireFormat : Grouping,
               A : Manifest : WireFormat,
               B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
    : DList[(K, (A, Option[B]))] = joinRight(d2, d1).map(v => (v._1, v._2.swap))


  /** Perform a co-group of two (2) distributed lists */
  def coGroup[K  : Manifest : WireFormat : Grouping,
              A : Manifest : WireFormat,
              B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      : DList[(K, (Iterable[A], Iterable[B]))] = {
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
}
