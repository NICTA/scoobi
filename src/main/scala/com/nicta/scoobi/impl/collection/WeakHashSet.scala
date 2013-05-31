package com.nicta.scoobi
package impl
package collection

import scala.runtime.AbstractFunction1
import scala.collection.mutable

/** A bare-bones implementation of a mutable `Set` that uses weak references
 *  to hold the elements.
 *
 *  Copied from the scala.reflect.internal.util code but without the AnyRef constraint
 */
class WeakHashSet[T] extends AbstractFunction1[T, Boolean] {
  private val underlying = mutable.HashSet[WeakReferenceWithEquals[T]]()

  /** Add the given element to this set. */
  def +=(elem: T): this.type = {
    underlying += new WeakReferenceWithEquals(elem)
    this
  }

  /** Does the given element belong to this set? */
  def contains(elem: T): Boolean =
    underlying.contains(new WeakReferenceWithEquals(elem))

  /** Does the given element belong to this set? */
  def apply(elem: T): Boolean = contains(elem)

  /** Return the number of elements in this set, including reclaimed elements. */
  def size = underlying.size
}


/** A WeakReference implementation that implements equals and hashCode by
 *  delegating to the referent.
 *
 *  Copied from the scala.reflect.internal.util code but without the AnyRef constraint
 */
class WeakReferenceWithEquals[T](ref: T) {
  def get(): T = underlying.get()

  override val hashCode = ref.hashCode

  override def equals(other: Any): Boolean = other match {
    case wf: WeakReferenceWithEquals[_] =>
      underlying.get() == wf.get()
    case _ =>
      false
  }

  private val underlying = new java.lang.ref.WeakReference(ref)
}
