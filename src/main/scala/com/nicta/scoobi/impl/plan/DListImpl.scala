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

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType

import core._
import comp._
import mapreducer._
import collection.Seqs._
import WireFormat._
import ManifestWireFormat._

/** An implementation of the DList trait that builds up a DAG of computation nodes. */
private[scoobi]
class DListImpl[A](comp: DComp[A])(implicit val mwf: ManifestWireFormat[A]) extends DList[A] {

  type C = DComp[A]

  def getComp: DComp[A] = comp

  def addSink(sink: Sink) =
    setComp((c: DComp[A]) => c.addSink(sink))

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) =
    setComp((c: DComp[A]) => c.updateSinks(sinks => sinks.updateLast(_.compressWith(codec, compressionType))))

  def setComp(f: DComp[A] => DComp[A]) = new DListImpl[A](f(comp))(mwf)

  def parallelDo[B : ManifestWireFormat, E : ManifestWireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(ParallelDo[A, B, E](Seq(comp), env.getComp, dofn,
                                      DoMapReducer(manifestWireFormat[A], manifestWireFormat[B], manifestWireFormat[E])))

  def ++(ins: DList[A]*): DList[A] = DListImpl.apply(comp +: ins.map(_.getComp))

  def groupByKey[K, V]
      (implicit ev:   A <:< (K, V),
                mwfk: ManifestWireFormat[K],
                gpk:  Grouping[K],
                mwfv: ManifestWireFormat[V]): DList[(K, Iterable[V])] = {
    implicit val mfk = mwfk.mf
    implicit val mfv = mwfv.mf
    implicit val wfk = mwfk.wf
    implicit val wfv = mwfv.wf

    new DListImpl(GroupByKey(comp, KeyValuesMapReducer(mwfk, gpk, mwfv)))(manifestAndWireFormat[(K, Iterable[V])])
  }

  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   A <:< (K,Iterable[V]),
                mwfk: ManifestWireFormat[K],
                gpk:  Grouping[K],
                mwfv: ManifestWireFormat[V]): DList[(K, V)] = new DListImpl(Combine(comp, f, KeyValueMapReducer(mwfk, gpk, mwfv)))

  lazy val materialize: DObject[Iterable[A]] = new DObjectImpl(Materialize(comp, SimpleMapReducer(manifestWireFormat[Iterable[A]]), comp.sinks))

  def parallelDo[B : ManifestWireFormat](dofn: DoFn[A, B]): DList[B] = parallelDo(UnitDObject.newInstance, dofn)

  override def toString = "\n"+ new ShowNode {}.pretty(comp)
}

private[scoobi]
object DListImpl {
  def apply[A : ManifestWireFormat](source: DataSource[_,_, A]): DListImpl[A] = apply(Seq(Load(source, SimpleMapReducer(manifestWireFormat[A]))))
  def apply[A : ManifestWireFormat](ins: Seq[CompNode]): DListImpl[A] =  {
    new DListImpl[A](
      ParallelDo[A, A, Unit](
        ins,
        UnitDObject.newInstance.getComp,
        new BasicDoFn[A, A] { def process(input: A, emitter: Emitter[A]) { emitter.emit(input) } },
        DoMapReducer(manifestWireFormat[A], manifestWireFormat[A], manifestAndWireFormat(implicitly[Manifest[Unit]], wireFormat[Unit]))))
  }

}
