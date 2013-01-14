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
import comp._
import source.SeqInput
import WireFormat._
import mapreducer._
import core.DList

/** A wrapper around an object that is part of the graph of a distributed computation.*/
private[scoobi]
class DObjectImpl[A](comp: ValueNode)(implicit val wf: WireFormat[A]) extends DObject[A] {
  type C = ValueNode

  def getComp: C = comp

  def map[B : WireFormat](f: A => B): DObject[B] =
    new DObjectImpl(Op(comp, Return.unit, (a: Any, any: Any) => f(a.asInstanceOf[A]), wireFormat[B]))

  def join[B : WireFormat](list: DList[B]): DList[(A, B)] = {
    val dofn = new DoFunction {
      def setupFunction(env: Any) {}
      def processFunction(env: Any, input: Any, emitter: EmitterWriter) { emitter.write((env, input)) }
      def cleanupFunction(env: Any, emitter: EmitterWriter) {}
    }
    new DListImpl(ParallelDo(Seq(list.getComp), comp, dofn, wireFormat[B], wireFormat[(A, B)], wireFormat[A]))
  }

  def toSingleElementDList: DList[A] = (this join SeqInput.fromSeq(Seq(()))).map(_._1)

  def join[B : WireFormat](o: DObject[B]): DObject[(A, B)] =
    DObjectImpl.tupled2((this, o))
}

object UnitDObject {
  def newInstance = new DObjectImpl[Unit](Return.unit)
}

private[scoobi]
object DObjectImpl {

  def apply[A : WireFormat](a: A) = new DObjectImpl[A](Return(a, wireFormat[A]))

  /* Implicit conversions from tuples of DObjects to DObject tuples. */
  def tupled2[T1 : WireFormat, T2 : WireFormat] (tup: (DObject[T1], DObject[T2])): DObject[(T1, T2)] =
    new DObjectImpl[(T1, T2)](Op(tup._1.getComp, tup._2.getComp, (a1: Any, a2: Any) => (a1.asInstanceOf[T1], a2.asInstanceOf[T2]), wireFormat[(T1, T2)]))

}
