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
package com.nicta.scoobi

import java.io._
import java.lang.Comparable
import org.apache.hadoop.io._
import annotation.implicitNotFound


/** Specify the way in which key-values are "shuffled". Used by 'groupByKey' in
  * 'DList'. */
@implicitNotFound(msg = "Cannot find Grouping type class for ${K}")
trait Grouping[K] {
  /** Specifies how key-values are partitioned among Reduer tasks. */
  def partition(key: K, num: Int): Int = (key.hashCode & Int.MaxValue) % num

  /** Specifies how values, for a given partition, are grouped together in
    * a given partition. */
  def groupCompare(x: K, y: K): Int = sortCompare(x, y)

  /** Specifies the order in which grouped values are presented to a Reducer
    * task, for a given partition. */
  def sortCompare(x: K, y: K): Int
}


/** Implicit definitions of Grouping instances for common types. */
object Grouping {

  /** An implicity Grouping type class instance where sorting is implemented via an Ordering
    * type class instance. Partitioning and grouping use the default implementation. */
  implicit def OrderingGrouping[T : Ordering] = new Grouping[T] {
    def sortCompare(x: T, y: T): Int = implicitly[Ordering[T]].compare(x, y)
  }

  /** An implicity Grouping type class instance where sorting is implemented via an Ordering
    * type class instance. Partitioning and grouping use the default implementation. */
  implicit def ComparableGrouping[T <: Comparable[T]] = new Grouping[T] {
    def sortCompare(x: T, y: T): Int = x.compareTo(y)
  }
}
