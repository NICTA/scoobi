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
import com.nicta.scoobi.impl.plan.comp._
import com.nicta.scoobi.impl.plan.comp.Load
import CollectFunctions._

import core._
import comp._
import mapreducer._
import collection.Seqs._
import WireFormat._
import WireFormat._
import com.nicta.scoobi.core
import com.nicta.scoobi.impl.control.Exceptions._
import com.nicta.scoobi.impl.plan.comp.Load
import com.nicta.scoobi.impl.plan.comp.Materialise
import com.nicta.scoobi.impl.plan.comp.Combine
import com.nicta.scoobi.impl.plan.comp.GroupByKey

/** An implementation of the DList trait that builds up a DAG of computation nodes. */
private[scoobi]
class DListImpl[A](comp: ProcessNode) extends DList[A] {

  type C = ProcessNode

  def getComp: C = comp

  import scalaz.Store

  def storeComp: Store[C, DList[A]] =
    Store(new DListImpl[A](_), comp)

  def addSink(sink: Sink) =
    storeComp puts (_.addSink(sink))

  def updateSinks(f: Seq[Sink] => Seq[Sink]) =
    storeComp puts (_.updateSinks(f))

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) =
    storeComp puts ((c: C) => c.updateSinks(sinks => sinks.updateLast(_.compressWith(codec, compressionType))))

  def parallelDo[B : WireFormat, E : WireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(ParallelDo.create(Seq(comp), env.getComp, dofn, wireFormat[A], wireFormat[B]))

  def ++(ins: DList[A]*): DList[A] = DListImpl.apply {
    val others = ins.map(_.getComp)
    comp match {
      // special case. If we just append processing nodes linked to sources
      // then we can create only one parallelDo node
      // this will avoid stack overflows when optimising the graph
      case ParallelDo(nodes, env, dofn, wa, wb, sinks, _) if others.forall(isParallelDo) =>
        val pds = others.collect(isAParallelDo)
        if (Return.isUnit(env)        && pds.forall(p => Return.isUnit(p.env)) &&
            comp.nodeSinks.isEmpty    && pds.forall(_.nodeSinks.isEmpty) &&
            dofn == EmitterDoFunction && pds.forall(_.dofn == EmitterDoFunction))
          nodes.toVector ++ pds.toVector
        else getComp +: others.toVector

      case other => other +: others.toVector
    }
  }

  def groupByKey[K, V]
      (implicit ev:   A <:< (K, V),
                wfk: WireFormat[K],
                gpk:  Grouping[K],
                wfv: WireFormat[V]): DList[(K, Iterable[V])] =
    new DListImpl(GroupByKey(comp, wfk, gpk, wfv))

  def combine[K, V]
      (f: Reduction[V])
      (implicit ev:   A <:< (K,Iterable[V]),
                wfk: WireFormat[K],
                wfv: WireFormat[V]): DList[(K, V)] =
    new DListImpl(Combine(comp, Combine.reducer((a1: Any, a2: Any) => f(a1.asInstanceOf[V], a2.asInstanceOf[V])), wfk, wfv, wfv))

  /**
   * Low-level combine to be able to emit arbitrary reduced values when the combiner is called
   */
  def combineDo[K, V, U](dofn: DoFn[Iterable[V], U])(implicit ev: A <:< (K, Iterable[V]), wfk: WireFormat[K], wfv: WireFormat[V], wfu: WireFormat[U]): DList[(K, U)] =
    new DListImpl(Combine(comp, dofn, wfk, wfv, wfu))

  lazy val materialise: DObject[Iterable[A]] = new DObjectImpl(Materialise(comp, wireFormat[Iterable[A]]))

  def parallelDo[B : WireFormat](dofn: DoFn[A, B]): DList[B] = parallelDo(UnitDObject.newInstance, dofn)

  override def toString = "\n"+ new ShowNode {}.pretty(comp)
}

private[scoobi]
object DListImpl {
  def apply[A](source: DataSource[_,_, A])(implicit wf: WireFormat[A]): DListImpl[A] = apply(Vector(Load(source, wf)))
  def apply[A](ins: Seq[CompNode])(implicit wf: WireFormat[A]): DListImpl[A] =  new DListImpl[A](ParallelDo.create(ins:_*)(wf))
}
