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
package measure

import scalazx.Reducer._
import scalaz._
import Scalaz._
import scalaz.Ordering._
import units._
import Quantity._
import time._
import RangeKey._

/**
 * @param key is the key for the measured object
 * @param startTime is when the measure took place
 * @param quantity is the main quantity measured (execution time for example)
 * @param secondaryQuantities are some adjacent quantities
 * @param fullName can be used to provide full information of where the measure comes from
 */
case class Measure[K : Semigroup](key: K, startTime: Long, quantity: Quantity, secondaryQuantities: Seq[Quantity] = Seq(), fullName: String = "") extends Timestamped {
  /** @return the name of the main quantity */
  def name = quantity.name
  /** add a timestamp to the key */
  def timestamp[T <: Timestamped](t: T) = copy(key=key+" ["+t.hhmmssS+"]")

  def show(v: Long) = (if (fullName.nonEmpty) fullName else quantity.name)+" = "+quantity.show(v)

  /** delegated methods */
  def value: Long = quantity.value
  def baseValue: Long = quantity.baseValue
  def unit: MeasureUnit = quantity.unit
  def withUnit(unit: MeasureUnit) = copy(quantity = quantity.withUnit(unit), secondaryQuantities = secondaryQuantities.map(_.withUnit(unit)))
  def quantities = quantity +: secondaryQuantities

  /** add a new secondary quantity */
  def add(n: String, value: Long): Measure[K] = add(Quantity(n, value, unit))
  /** add a new secondary quantity */
  def add(q: Quantity): Measure[K] = copy(secondaryQuantities = secondaryQuantities :+ q)
  /** divide all the quantities by a number */
  def divideBy(i: Int): Measure[K] = copy(quantity = quantity.divideBy(i), secondaryQuantities = secondaryQuantities.map(_.divideBy(i)))

  /** this is only used for testing */
  def increment(i: Long) = copy(quantity = quantity.increment(i), secondaryQuantities = secondaryQuantities.map(_.increment(i)))
}

object Measure {
  /** constructor for a single quantity */
  def apply[K : Semigroup](name: String, key: K, value: Long): Measure[K] = new Measure(key, 0, Quantity(name, value))

  /** this key is used to create the graphs */
  def measureKey[K] = (_:Measure[K]).key
  /** measures can be ordered according to their key */
  def keyOrder[K : Order] = order((_:Measure[K]).key)
  /** measures can be ordered according to their main quantity */
  def valueOrder[K] = order((_:Measure[K]).quantity)(quantityOrder)
  /** measures can be ordered according to their timestamp */
  def startTimeOrder[K] = order((_:Measure[K]).startTime)
  /** measures can be left unordered */
  def noOrder[K] = order((m:Measure[K]) => 0L)
  /** weighted measures can be ordered according to their main quantity */
  def weightedValueOrder[K] = order((weightedMeasure:(Int, Measure[K])) => weightedMeasure._2.quantity)(quantityOrder)

  implicit def stringKeyIsSemigroup: Semigroup[StringKey] = new Semigroup[StringKey] {
    def append(s1: StringKey, s2: =>StringKey) = if (s1 == s2) s1 else s2
  }
  /** measures can be added */
  implicit def measureIsSemiGroup[K : Semigroup] = new Semigroup[Measure[K]] {
    val keySemigroup = implicitly[Semigroup[K]]

    def append(m1: Measure[K], m2: =>Measure[K]) = {
      val appendSecondary = m1.secondaryQuantities.zip(m2.secondaryQuantities).foldLeft(Seq[Quantity]()) {
        case (res, (q1, q2)) => res :+ quantityIsSemigroup.append(q1, q2)
      }
      m1.copy(key = keySemigroup.append(m1.key, m2.key), quantity = quantityIsSemigroup.append(m1.quantity, m2.quantity), secondaryQuantities = appendSecondary)
    }
  }

  /** measures can be added with a given weight */
  implicit def measureIsWeightedSemiGroup[K : Semigroup] = new Semigroup[(Int, Measure[K])] {
    val keySemigroup = implicitly[Semigroup[K]]

    def append(n1m1: (Int, Measure[K]), n2m2: =>(Int, Measure[K])) = {
      val (n1, m1) = n1m1
      val (n2, m2) = n2m2
      val appendSecondary = m1.secondaryQuantities.zip(m2.secondaryQuantities).foldLeft(Seq[Quantity]()) {
        case (res, (q1, q2)) => res :+ quantityIsSemigroup.append(q1, q2)
      }
      (n1+n2, m1.copy(key = keySemigroup.append(m1.key, m2.key),
                      quantity = quantityIsSemigroup.append(m1.quantity, m2.quantity),
                      secondaryQuantities = appendSecondary))
    }
  }

  /**
   * @return a function providing a timestamped measure from a function returning a measure
   */
  implicit def toTimestamped[T <: Timestamped, K : Semigroup]( f: T => Measure[K]): ToTimestamped[T, K] = new ToTimestamped(f)
  class ToTimestamped[T <: Timestamped, K : Semigroup]( f: T => Measure[K]) {
    def timestamped = (t:T) => f(t).timestamp(t)
  }

  implicit def toDistributed[T, K : Semigroup]( f: T => Measure[K]): ToDistributed[T, K] = new ToDistributed(f)
  class ToDistributed[T, K : Semigroup]( f: T => Measure[K]) {
    def distributed(rangeSize: Int, unit: MeasureUnit) = (t: T) => {
      val measured = f(t)
      Measure("number of calls", range(measured.value, rangeSize, unit), 1).copy(startTime = measured.startTime)
    }

  }
}
