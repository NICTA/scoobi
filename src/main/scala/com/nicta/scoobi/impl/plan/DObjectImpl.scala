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
package impl
package plan

import core._

/** A wrapper around an object that is part of the graph of a distributed computation.*/
class DObjectImpl[A : Manifest : WireFormat] private[scoobi] (comp: Smart.DComp[A, Exp]) extends DObject[A] {

  val m = implicitly[Manifest[A]]
  val wf = implicitly[WireFormat[A]]

  private[scoobi]
  def this(x: A) = this(Smart.Return(x))

  private[scoobi]
  def getComp: Smart.DComp[A, Exp] = comp

  def map[B : Manifest : WireFormat](f: A => B): DObject[B] =
    new DObjectImpl(Smart.Op(comp, Smart.Return(()), (a: A, _: Unit) => f(a)))

  def join[B : Manifest : WireFormat](list: DList[B]): DList[(A, B)] = {
    val dofn = new EnvDoFn[B, (A, B), A] {
      def setup(env: A) {}
      def process(env: A, input: B, emitter: Emitter[(A, B)]) { emitter.emit((env, input)) }
      def cleanup(env: A, emitter: Emitter[(A, B)]) {}
    }
    new DListImpl(Smart.ParallelDo(list.getComp, comp, dofn))
  }
}


object UnitDObject extends DObjectImpl(())


/** */
object DObjectImpl {

  /* Implicit conversions from tuples of DObjects to DObject tuples. */
  def tupled2[T1 : Manifest : WireFormat, T2 : Manifest : WireFormat] (tup: (DObject[T1], DObject[T2])): DObject[(T1, T2)] =
      new DObjectImpl(Smart.Op(tup._1.getComp, tup._2.getComp, (a: T1, b: T2) => (a, b)))
}
