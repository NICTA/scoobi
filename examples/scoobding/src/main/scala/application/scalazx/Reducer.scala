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
package scalazx

import scalaz.{Semigroup, Scalaz, std, Order}
import Scalaz._
import std._
import iterable._
import map._

/**
 * The methods below provide way to:
 *
 * - find the maximum of some elements having by key
 * - cumulate the values of some elements for each key and take the maximum by key
 *
 * To do that, we just need to provide:
 *
 * - a key: T => K (per method call, per server)
 * - a Semigroup[T] (to compute the sum)
 * - an Order[T] (to get the max, but it could be the min)
 */
trait Reducer {
  /**
   * @return a seq of values where we add values per key.
   *
   * The "add" operation depends on the Semigroup, so it could be "+" but also "compare"
   *
   */
  def addByKey[T : Semigroup, K : Semigroup](values: Seq[T], key: T => K): Seq[T] = {
    // sort the values by key
    val valuesByKey: Seq[(K, T)] = values.view.map(r => (key(r), r))
    // add them by key, using the Option[T] Monoid
    val sumByKey = valuesByKey.foldMap { case (k, v) => Map((k, Option(v))) }
    // keep the values only and transform a Seq[Option[T]] into an Option[Seq[T]] with `sequence`
    sumByKey.map(_._2).toList.sequence.getOrElse(Seq())
  }

  /**
   * @return a seq of values where we add values per key, then we sort the results by maximum
   */
  def sumByKey[T : Semigroup : Order, K : Semigroup](values: Seq[T], key: T => K): Seq[T] =
    addByKey(values, key).sorted.reverse

  /**
   * @return a seq of values where we only keep the maximum value per key, then we sort the results by maximum
   */
  def maxByKey[T : Order, K : Semigroup](records: Seq[T], key: T => K): Seq[T] =
    addByKey(records, key)(maxIsSemigroup, Semigroup[K]).sorted.reverse

  /**
   * @return an Ordering based on an existing ordering for the property of an object
   */
  def order[T, U : Order](property: T => U) = new Order[T] {
    def order(x: T, y: T) = Order[U].order(property(x), property(y))
  }

  /**
   * create a reverse Order from an existing one
   */
  implicit def reversedOrder[T](o: Order[T]) = new ReversedOrder(o)
  class ReversedOrder[T](o: Order[T]) {
    def reverse = new Order[T] {
       def order(x: T, y: T) = o.order(y, x)
    }
  }

  /**
   * if there's an Order defined on T, we can derive a Semigroup for T elements so that
   * "adding" 2 elements only keeps the maximum
   */
  def maxIsSemigroup[T : Order] = new Semigroup[T] {
    def append(t1: T, t2 : =>T): T = Order[T].max(t1, t2)
  }

  implicit def orderToOrdering[T : Order]: scala.math.Ordering[T] = new scala.math.Ordering[T] {
    def compare(x: T, y: T) = Order[T].order(x, y).toInt
  }
}

object Reducer extends Reducer