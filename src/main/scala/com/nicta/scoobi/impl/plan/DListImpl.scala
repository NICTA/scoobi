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
import io.{Sink, DataSink, DataSource}
import WireFormat._
import ManifestWireFormat._

/** An implemenentation of the DList trait that builds up a DAG of computation nodes. */
class DListImpl[A] private[scoobi] (comp: DComp[A])(implicit val mwf: ManifestWireFormat[A]) extends DList[A] {

  private[scoobi]
  def getComp: DComp[A] = comp

  private[scoobi]
  def setComp(f: DComp[A] => DComp[A]) = new DListImpl[A](f(comp))(mwf)

  def parallelDo[B : ManifestWireFormat, E : ManifestWireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(ParallelDo[A, B, E](comp, env.getComp, dofn,
                                      DoIO(manifestWireFormat[A], manifestWireFormat[B], manifestWireFormat[E])))

  def ++(ins: DList[A]*): DList[A] = new DListImpl[A](Flatten[A](comp +: ins.map(_.getComp).toList, StraightIO(manifestWireFormat[A])))

  def groupByKey[K, V]
      (implicit ev:   DComp[A] <:< DComp[(K, V)],
                mwfk: ManifestWireFormat[K],
                gpk:  Grouping[K],
                mwfv: ManifestWireFormat[V]): DList[(K, Iterable[V])] = {
    implicit val mfk = mwfk.mf
    implicit val mfv = mwfv.mf
    implicit val wfk = mwfk.wf
    implicit val wfv = mwfv.wf

    new DListImpl(GroupByKey(comp, KeyValuesIO(mwfk, gpk, mwfv)))(manifestAndWireFormat[(K, Iterable[V])])
  }

  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   DComp[A] <:< DComp[(K,Iterable[V])],
                mwfk: ManifestWireFormat[K],
                gpk:  Grouping[K],
                mwfv: ManifestWireFormat[V]): DList[(K, V)] = new DListImpl(Combine(comp, f, KeyValueIO(mwfk, gpk, mwfv)))

  def materialize: DObject[Iterable[A]] = new DObjectImpl(Materialize(comp, StraightIO(manifestWireFormat[A])))

  def groupBarrier: DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() {}
      def process(input: A, emitter: Emitter[A]) { emitter.emit(input) }
      def cleanup(emitter: Emitter[A]) {}
    }
    new DListImpl(ParallelDo(comp, UnitDObject.getComp, dofn, DoIO(mwf, mwf, manifestWireFormat[Unit]), Barriers(groupBarrier = true, fuseBarrier = false)))
  }
}

private[scoobi]
object DListImpl {
  def apply[A : ManifestWireFormat](source: DataSource[_,_, A]) =  {
    new DListImpl[A](
      ParallelDo[A, A, Unit](
        Load(source, StraightIO(manifestWireFormat[A])),
        UnitDObject.getComp,
        new BasicDoFn[A, A] { def process(input: A, emitter: Emitter[A]) { emitter.emit(input) } },
        DoIO(manifestWireFormat[A], manifestWireFormat[A], manifestAndWireFormat(implicitly[Manifest[Unit]], wireFormat[Unit]))))
  }

}
