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
  /** Persisting */
  def persist[A](o: DObject[A])(implicit sc: core.ScoobiConfiguration) =     { sc.persist(o) }
  def persist[A](list: core.DList[A])(implicit sc: core.ScoobiConfiguration) = { sc.persist(list) }
  def persist[A](ps: Persistent[_]*)(implicit sc: core.ScoobiConfiguration)  = { sc.persist(ps) }

  /**
   * run a list.
   *
   * This is equivalent to:
   *   ```
   *    val obj = list.materialise
   *    run(obj)
   *   ```
   */
  def run[T](p: core.Persistent[T])(implicit configuration: core.ScoobiConfiguration): T = {
    p match {
      case list: core.DList[_] => Vector(persist(list.materialise).toSeq:_*)
      case o: DObject[_]       => persist(o)
    }
  }

  def run[T1, T2](p1: Persistent[T1], p2 : Persistent[T2])(implicit configuration: core.ScoobiConfiguration): (T1, T2) = {
    outer.persist(p1, p2)
    (run(p1), run(p2))
  }

  def run[T1, T2, T3](p1: Persistent[T1], p2: Persistent[T2], p3: Persistent[T3])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3) = {
    outer.persist(p1, p2, p3)
    (run(p1), run(p2), run(p3))
  }

  def run[T1, T2, T3, T4](p1: Persistent[T1], p2: Persistent[T2], p3: Persistent[T3], p4: Persistent[T4])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4) = {
    outer.persist(p1, p2, p3, p4)
    (run(p1), run(p2), run(p3), run(p4))
  }

  def run[T1, T2, T3, T4, T5](p1: Persistent[T1],
                              p2: Persistent[T2],
                              p3: Persistent[T3],
                              p4: Persistent[T4],
                              p5: Persistent[T5])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5) = {
    outer.persist(p1, p2, p3, p4, p5)
    (run(p1), run(p2), run(p3), run(p4), run(p5))
  }

  def run[T1, T2, T3, T4, T5, T6](p1: Persistent[T1],
                                  p2: Persistent[T2],
                                  p3: Persistent[T3],
                                  p4: Persistent[T4],
                                  p5: Persistent[T5],
                                  p6: Persistent[T6])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5, T6) = {
    outer.persist(p1, p2, p3, p4, p5, p6)
    (run(p1), run(p2), run(p3), run(p4), run(p5), run(p6))
  }

  def run[T1, T2, T3, T4, T5, T6, T7](p1: Persistent[T1],
                                      p2: Persistent[T2],
                                      p3: Persistent[T3],
                                      p4: Persistent[T4],
                                      p5: Persistent[T5],
                                      p6: Persistent[T6],
                                      p7: Persistent[T7])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5, T6, T7) = {
    outer.persist(p1, p2, p3, p4, p5, p6, p7)
    (run(p1), run(p2), run(p3), run(p4), run(p5), run(p6), run(p7))
  }

  def run[T1, T2, T3, T4, T5, T6, T7, T8](p1: Persistent[T1],
                                          p2: Persistent[T2],
                                          p3: Persistent[T3],
                                          p4: Persistent[T4],
                                          p5: Persistent[T5],
                                          p6: Persistent[T6],
                                          p7: Persistent[T7],
                                          p8: Persistent[T8])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5, T6, T7, T8) = {
    outer.persist(p1, p2, p3, p4, p5, p6, p7, p8)
    (run(p1), run(p2), run(p3), run(p4), run(p5), run(p6), run(p7), run(p8))
  }

  def run[T1, T2, T3, T4, T5, T6, T7, T8, T9](p1: Persistent[T1],
                                              p2: Persistent[T2],
                                              p3: Persistent[T3],
                                              p4: Persistent[T4],
                                              p5: Persistent[T5],
                                              p6: Persistent[T6],
                                              p7: Persistent[T7],
                                              p8: Persistent[T8],
                                              p9: Persistent[T9])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5, T6, T7, T8, T9) = {
    outer.persist(p1, p2, p3, p4, p5, p6, p7, p8, p9)
    (run(p1), run(p2), run(p3), run(p4), run(p5), run(p6), run(p7), run(p8), run(p9))
  }

  def run[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](p1: Persistent[T1],
                                                   p2: Persistent[T2],
                                                   p3: Persistent[T3],
                                                   p4: Persistent[T4],
                                                   p5: Persistent[T5],
                                                   p6: Persistent[T6],
                                                   p7: Persistent[T7],
                                                   p8: Persistent[T8],
                                                   p9: Persistent[T9],
                                                   p10: Persistent[T10])(implicit configuration: core.ScoobiConfiguration): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) = {
    outer.persist(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)
    (run(p1), run(p2), run(p3), run(p4), run(p5), run(p6), run(p7), run(p8), run(p9), run(p10))
  }

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

}
object Persist extends Persist
