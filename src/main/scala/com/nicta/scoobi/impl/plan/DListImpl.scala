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
import WireFormat._
import com.nicta.scoobi.core

/** An implementation of the DList trait that builds up a DAG of computation nodes. */
private[scoobi]
class DListImpl[A](comp: ProcessNode) extends DList[A] {

  type C = ProcessNode

  def getComp: C = comp

  def addSink(sink: Sink) =
    setComp((c: C) => c.addSink(sink))

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) =
    setComp((c: C) => c.updateSinks(sinks => sinks.updateLast(_.compressWith(codec, compressionType))))

  def setComp(f: C => C) = new DListImpl[A](f(comp))

  def parallelDo[B : WireFormat, E : WireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(ParallelDo(Seq(comp), env.getComp, dofn, wireFormat[A], wireFormat[B]))

  def ++(ins: DList[A]*): DList[A] = DListImpl.apply(comp +: ins.map(_.getComp))

  def groupByKey[K, V]
      (implicit ev:   A <:< (K, V),
                wfk: WireFormat[K],
                gpk:  Grouping[K],
                wfv: WireFormat[V]): DList[(K, Iterable[V])] = {

    new DListImpl(GroupByKey(comp, wfk, gpk, wfv))
  }

  def combine[K, V]
      (f: Reduction[V])
      (implicit ev:   A <:< (K,Iterable[V]),
                wfk: WireFormat[K],
                wfv: WireFormat[V]): DList[(K, V)] = new DListImpl(Combine(comp, (a1: Any, a2: Any) => f(a1.asInstanceOf[V], a2.asInstanceOf[V]), wfk, wfv))

  lazy val materialise: DObject[Iterable[A]] = new DObjectImpl(Materialise(comp, wireFormat[Iterable[A]]))

  def parallelDo[B : WireFormat](dofn: DoFn[A, B]): DList[B] = parallelDo(UnitDObject.newInstance, dofn)

  override def toString = "\n"+ new ShowNode {}.pretty(comp)
}

private[scoobi]
object DListImpl {
  def apply[A](source: DataSource[_,_, A])(implicit wf: WireFormat[A]): DListImpl[A] = apply(Seq(Load(source, wf)))
  def apply[A](ins: Seq[CompNode])(implicit wf: WireFormat[A]): DListImpl[A] =  new DListImpl[A](ParallelDo.create(ins:_*)(wf))
}
