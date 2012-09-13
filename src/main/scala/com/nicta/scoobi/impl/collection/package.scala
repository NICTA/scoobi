package com.nicta.scoobi
package impl

import scala.collection.immutable.{SortedSet, TreeSet}

package object collection {
  object IdSet {
    implicit def byId[T <: { def id: Int }] = Ordering.by((_:T).id)

    def apply[T <: { def id: Int }](seq: T*): SortedSet[T] = {
      val r = seq.foldLeft(empty[T]) { (res, cur) => res + cur }
      r
    }
    def empty[T <: { def id: Int }] = new TreeSet[T]()(byId)
  }

  implicit def ToIdSet[T <: { def id: Int }](seq: Seq[T]): ToIdSet[T] = new ToIdSet(seq)
  class ToIdSet[T <: { def id: Int }](seq: Seq[T]) {
    def toIdSet = IdSet.apply(seq:_*)
  }

}


