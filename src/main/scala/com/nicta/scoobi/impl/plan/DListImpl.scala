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
import io.DataSource


/** An implemenentation of the DList trait that builds up a DAG of computation nodes. */
class DListImpl[A : Manifest : WireFormat] private[scoobi] (comp: Smart.DComp[A, Arr]) extends DList[A] {

  val m = implicitly[Manifest[A]]
  val wf = implicitly[WireFormat[A]]

  private[scoobi]
  def this(source: DataSource[_, _, A]) = {
    this(Smart.ParallelDo(
      Smart.Load(source),
      UnitDObject.getComp,
      new BasicDoFn[A, A] { def process(input: A, emitter: Emitter[A]) { emitter.emit(input) } }))
  }

  private[scoobi]
  def getComp: Smart.DComp[A, Arr] = comp

  def parallelDo[B : Manifest : WireFormat, E : Manifest : WireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B] =
    new DListImpl(Smart.ParallelDo(comp, env.getComp, dofn))

  def ++(ins: DList[A]*): DList[A] = new DListImpl(Smart.Flatten(List(comp) ::: ins.map(_.getComp).toList))

  def groupByKey[K, V]
      (implicit ev:   Smart.DComp[A, Arr] <:< Smart.DComp[(K, V), Arr],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                grpK: Grouping[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, Iterable[V])] = new DListImpl(Smart.GroupByKey(comp))

  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   Smart.DComp[A, Arr] <:< Smart.DComp[(K,Iterable[V]), Arr],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                grpK: Grouping[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, V)] = new DListImpl(Smart.Combine(comp, f))

  def materialise: DObject[Iterable[A]] = new DObjectImpl(Smart.Materialise(comp))

  def groupBarrier: DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() {}
      def process(input: A, emitter: Emitter[A]) { emitter.emit(input) }
      def cleanup(emitter: Emitter[A]) {}
    }
    new DListImpl(Smart.ParallelDo(comp, UnitDObject.getComp, dofn, groupBarrier = true))
  }
}
