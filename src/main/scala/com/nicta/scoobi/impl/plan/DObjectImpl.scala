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

import comp._
import core._
import WireFormat._

/** A wrapper around an object that is part of the graph of a distributed computation.*/
private[scoobi]
class DObjectImpl[A](comp: CompNode)(implicit val mf: Manifest[A], val wf: WireFormat[A]) extends DObject[A] {

  private[scoobi]
  def getComp: CompNode = comp

  def map[B : Manifest : WireFormat](f: A => B): DObject[B] =
    new DObjectImpl(Op(comp, Return((), manifest[Unit], wireFormat[Unit]), (a: A, _: Unit) => f(a), manifest[B], wireFormat[B]))

  def join[B : Manifest : WireFormat](list: DList[B]): DList[(A, B)] = {
    val dofn = new EnvDoFn[B, (A, B), A] {
      def setup(env: A) {}
      def process(env: A, input: B, emitter: Emitter[(A, B)]) { emitter.emit((env, input)) }
      def cleanup(env: A, emitter: Emitter[(A, B)]) {}
    }
    new DListImpl(ParallelDo[B, (A, B), A](list.getComp, comp, dofn, false, false, manifest[B], wireFormat[B], manifest[(A, B)], wireFormat[(A, B)], manifest[A], wireFormat[A]))
  }
}


object UnitDObject extends DObjectImpl[Unit](Return.unit)

private[scoobi]
object DObjectImpl {
  def apply[A : Manifest : WireFormat](a: A) = new DObjectImpl(Return(a, manifest[A], wireFormat[A]))

  /* Implicit conversions from tuples of DObjects to DObject tuples. */
  def tupled2[T1 : Manifest : WireFormat, T2 : Manifest : WireFormat] (tup: (DObject[T1], DObject[T2])): DObject[(T1, T2)] =
    new DObjectImpl(Op(tup._1.getComp, tup._2.getComp, (a: T1, b: T2) => (a, b), manifest[(T1, T2)], wireFormat[(T1, T2)]))
}
