package com.nicta.scoobi
package application

import core.{Persistent, DObject}

/**
 * This trait provides operations and implicit definitions to persist DLists and DObjects.
 *
 * Generally `persist` on a DList or a DObject executes the computations but doesn't return any value while `run`
 * return values, triggering the computations if necessary (but not if persist has already been called)
 *
 * Several DLists and DObjects which are part of the same logical computation graph can be persisted jointly by calling
 * `persist(objects and lists)`
 */
trait Persist { outer =>

  /** make a DList runnable, executing the computation and returning the values */
  implicit def asRunnableDList[T](list: core.DList[T]) = new RunnableDList(list)
  case class RunnableDList[T](list: core.DList[T]) {
    def run(implicit configuration: core.ScoobiConfiguration) = outer.run(list)
  }

  /** make a DObject runnable, executing the computation and returning the values */
  implicit def asRunnableDObject[T](o: DObject[T]) = new RunnableDObject(o)
  case class RunnableDObject[T](o: DObject[T]) {
    def run(implicit configuration: core.ScoobiConfiguration) = outer.run(o)
  }

  /**
   * run a list.
   *
   * This is equivalent to:
   *   ```
   *    val obj = list.materialize
   *    run(obj)
   *   ```
   */
  def run[T](list: =>core.DList[T])(implicit configuration: core.ScoobiConfiguration): Seq[T] =
    Vector(run(list.materialize).toSeq:_*)

  /**
   * run a DObject, executing the computation before if necessary (i.e. calling `persist` on the object)
   */
  def run[T](o: DObject[T])(implicit configuration: core.ScoobiConfiguration): T =
    outer.persist(o)

  /** allow to call `list.persist` */
  implicit def persistableList[A](list: core.DList[A]): PersistableList[A] = new PersistableList(list)
  class PersistableList[A](list: core.DList[A]) {
    def persist(implicit sc: core.ScoobiConfiguration) = outer.persist(list)
  }
  /** allow to call `object.persist` */
  implicit def persistableObject[A](o: DObject[A]): PersistableObject[A] = new PersistableObject(o)
  class PersistableObject[A](o: DObject[A]) {
    def persist(implicit sc: core.ScoobiConfiguration) = outer.persist(o)
  }

  /** Persisting */
  def persist[A](o: DObject[A])(implicit sc: core.ScoobiConfiguration) =     { sc.persist(o) }
  def persist[A](list: core.DList[A])(implicit sc: core.ScoobiConfiguration) { sc.persist(list) }
  def persist[A](ps: Persistent*)(implicit sc: core.ScoobiConfiguration)     { sc.persist(ps) }
}
object Persist extends Persist
