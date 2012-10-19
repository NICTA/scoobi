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
import io.DataSource
import WireFormat._

/** An implemenentation of the DList trait that builds up a DAG of computation nodes. */
class DListImpl[A] private[scoobi] (comp: CompNode)(implicit val mf: Manifest[A], val wf: WireFormat[A]) extends DList[A] {

  private[scoobi]
  def getComp: CompNode = comp

  def parallelDo[B : Manifest : WireFormat, E : Manifest : WireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(ParallelDo[A, B, E](comp, env.getComp, dofn, groupBarrier = false, fuseBarrier = false, manifest[A], wireFormat[A], manifest[B], wireFormat[B], manifest[E], wireFormat[E]))

  def ++(ins: DList[A]*): DList[A] = new DListImpl(Flatten(List(comp) ::: ins.map(_.getComp).toList, mf, wf))

  def groupByKey[K, V]
      (implicit ev:   DComp[A, Arr] <:< DComp[(K, V), Arr],
                mfk:  Manifest[K],
                wfk:  WireFormat[K],
                gpk:  Grouping[K],
                mfv:  Manifest[V],
                wfv:  WireFormat[V]): DList[(K, Iterable[V])] = new DListImpl(GroupByKey(comp, mfk, wfk, gpk, mfv, wfv))

  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   DComp[A, Arr] <:< DComp[(K,Iterable[V]), Arr],
                mfk:  Manifest[K],
                wfk:  WireFormat[K],
                gpk:  Grouping[K],
                mfv:  Manifest[V],
                wfv:  WireFormat[V]): DList[(K, V)] = new DListImpl(Combine(comp, f, mfk, wfk, gpk, mfv, wfv))

  def materialize: DObject[Iterable[A]] = new DObjectImpl(Materialize(comp, mf, wf))

  def groupBarrier: DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() {}
      def process(input: A, emitter: Emitter[A]) { emitter.emit(input) }
      def cleanup(emitter: Emitter[A]) {}
    }
    new DListImpl(ParallelDo(comp, UnitDObject.getComp, dofn, groupBarrier = true, fuseBarrier = false, mf, wf, mf, wf, manifest[Unit], wireFormat[Unit]))
  }
}

private[scoobi]
object DListImpl {
  def apply[A : Manifest : WireFormat](source: DataSource[_, _, A]) =  {
    new DListImpl(ParallelDo[A, A, Unit](Load(source), UnitDObject.getComp,
      new BasicDoFn[A, A] { def process(input: A, emitter: Emitter[A]) { emitter.emit(input) } },
      groupBarrier = false, fuseBarrier = false,
      manifest[A], wireFormat[A],
      manifest[A], wireFormat[A],
      manifest[Unit], wireFormat[Unit]))
  }

}
