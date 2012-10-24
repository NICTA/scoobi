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
class DObjectImpl[A](comp: CompNode)(implicit val mwf: ManifestWireFormat[A]) extends DObject[A] {

  private[scoobi]
  def getComp: CompNode = comp

  def map[B : ManifestWireFormat](f: A => B): DObject[B] =
    new DObjectImpl(Op(comp, Return.unit, (a: A, _: Unit) => f(a), StraightIO(manifestWireFormat[B])))

  def join[B : ManifestWireFormat](list: DList[B]): DList[(A, B)] = {
    val dofn = new EnvDoFn[B, (A, B), A] {
      def setup(env: A) {}
      def process(env: A, input: B, emitter: Emitter[(A, B)]) { emitter.emit((env, input)) }
      def cleanup(env: A, emitter: Emitter[(A, B)]) {}
    }
    new DListImpl(ParallelDo[B, (A, B), A](list.getComp, comp, dofn, DoIO(manifestWireFormat[B], manifestWireFormat[(A, B)], manifestWireFormat[A])))
  }
}

object UnitDObject extends DObjectImpl[Unit](Return.unit)

private[scoobi]
object DObjectImpl {

  def apply[A : ManifestWireFormat](a: A) = new DObjectImpl[A](Return(a, StraightIO(manifestWireFormat[A])))

  /* Implicit conversions from tuples of DObjects to DObject tuples. */
  def tupled2[T1 : ManifestWireFormat, T2 : ManifestWireFormat] (tup: (DObject[T1], DObject[T2])): DObject[(T1, T2)] =
    new DObjectImpl[(T1, T2)](Op(tup._1.getComp, tup._2.getComp, (a: T1, b: T2) => (a, b), StraightIO(manifestWireFormat[(T1, T2)])))

}
