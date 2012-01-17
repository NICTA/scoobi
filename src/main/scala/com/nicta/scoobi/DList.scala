/**
  * Copyright 2011 National ICT Australia Limited
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

import scala.collection.mutable.{Map => MMap}

import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.Smart.ConvertInfo
import com.nicta.scoobi.impl.plan.MSCRGraph
import com.nicta.scoobi.impl.exec.Executor


/** A list that is distributed across multiple machines. */
class DList[A : Manifest : WireFormat](private val ast: Smart.DList[A]) {

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Primitive functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Apply a specified function to "chunks" of elements from the distributed list to produce
    * zero or more output elements. The resulting output elements from the many "chunks" form
    * a new distributed list. */
  def parallelDo[B] (dofn: DoFn[A, B])(implicit mB:  Manifest[B], wtB: WireFormat[B]): DList[B] =
    new DList(Smart.ParallelDo(ast, dofn))

  /** Concatenate one or more distributed lists to this distributed list. */
  def ++(ins: DList[A]*): DList[A] = new DList(Smart.Flatten(List(ast) ::: ins.map(_.ast).toList))

  /** Group the values of a distributed list with key-value elements by key. */
  def groupByKey[K, V]
      (implicit ev:   Smart.DList[A] <:< Smart.DList[(K, V)],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                ordK: Ordering[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, Iterable[V])] = new DList(Smart.GroupByKey(ast))

  /** Apply an associative function to reduce the collection of values to a single value in a
    * key-value-collection distributed list. */
  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   Smart.DList[A] <:< Smart.DList[(K,Iterable[V])],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                ordK: Ordering[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, V)] = new DList(Smart.Combine(ast, f))


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derived functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** For each element of the distributed list produce zero or more elements by
    * applying a specified function. The resulting collection of elements form a
    * new distributed list. */
  def flatMap[B : Manifest : WireFormat](f: A => Iterable[B]): DList[B] = {
    val dofn = new DoFn[A, B] {
      def setup() = {}
      def process(input: A, emitter: Emitter[B]) = f(input).foreach { emitter.emit(_) }
      def cleanup(emitter: Emitter[B]) = {}
    }
    parallelDo(dofn)
  }

  /** For each element of the distributed list produce a new element by applying a
    * specified function. The resulting collection of elements form a new
    * distributed list. */
  def map[B : Manifest : WireFormat](f: A => B): DList[B] = {
    val dofn = new DoFn[A, B] {
      def setup() = {}
      def process(input: A, emitter: Emitter[B]) = emitter.emit(f(input))
      def cleanup(emitter: Emitter[B]) = {}
    }
    parallelDo(dofn)
  }

  /** Keep elements from the distributed list that pass a specified predicate function. */
  def filter(p: A => Boolean): DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() = {}
      def process(input: A, emitter: Emitter[A]) = if (p(input)) { emitter.emit(input) }
      def cleanup(emitter: Emitter[A]) = {}
    }
    parallelDo(dofn)
  }

  /** Keep elements from the distributed list that do not pass a specified
    * predicate function. */
  def filterNot(p: A => Boolean): DList[A] = filter(p andThen (! _))

  /** Build a new DList by applying a partial function to all elements of this DList on
    * which the function is defined. */
  def collect[B : Manifest : WireFormat](pf: PartialFunction[A, B]): DList[B] = {
    val dofn = new DoFn[A, B] {
      def setup() = {}
      def process(input: A, emitter: Emitter[B]) = if (pf.isDefinedAt(input)) { emitter.emit(pf(input)) }
      def cleanup(emitter: Emitter[B]) = {}
    }
    parallelDo(dofn)
  }

  /** Group the values of a distributed list according to some discriminator function. */
  def groupBy[K : Manifest : WireFormat : Ordering](f: A => K): DList[(K, Iterable[A])] =
    map(x => (f(x), x)).groupByKey

  /** Partitions this distributed list into a pair of distributed lists according to some
    * predicate. The first distributed list consists of elements that satisfy the predicate
    * and the second of all elements that don't. */
  def partition(p: A => Boolean): (DList[A], DList[A]) = (filter(p), filterNot(p))

  /** Converts a distributed list of iterable values into to a distributed list in which
    * all the values are concatenated. */
  def flatten[B](implicit ev: A <:< Iterable[B], mB: Manifest[B], wtB: WireFormat[B]): DList[B] = {
    val dofn = new DoFn[A, B] {
      def setup() = {}
      def process(input: A, emitter: Emitter[B]) = input.foreach { emitter.emit(_) }
      def cleanup(emitter: Emitter[B]) = {}
    }
    parallelDo(dofn)
  }

  /** Create a new distributed list that is keyed based on a specified function. */
  def by[K : Manifest : WireFormat](kf: A => K): DList[(K, A)] = map { x => (kf(x), x) }
}


/** A class that specifies how to make a distributed list persistent. */
class DListPersister[A](val dl: DList[A], val persister: Persister[A])


object DList {

  /** Persist one or more distributed lists. */
  def persist(outputs: DListPersister[_]*) = {

    /* Produce map of all unique outputs and their corresponding persisters. */
    val rawOutMap: Map[Smart.DList[_], Set[Persister[_]]] = {
      val emptyM: Map[Smart.DList[_], Set[Persister[_]]] = Map()
      outputs.foldLeft(emptyM)((m,p) => m + ((p.dl.ast, m.getOrElse(p.dl.ast, Set()) + p.persister)))
    }

    /* Optimise the plan associated with the outpus. */
    val outMap : Map[Smart.DList[_], Set[Persister[_]]] = {
      val optOuts: List[Smart.DList[_]] = Smart.optimisePlan(rawOutMap.keys.toList)
      (rawOutMap.toList zip optOuts) map { case ((_, p), o) => (o, p) } toMap
    }

    val ds = outMap.keys

    import com.nicta.scoobi.impl.plan.{Intermediate => I}
    val iMSCRGraph: I.MSCRGraph = I.MSCRGraph(ds)
    val ci = ConvertInfo(outMap, iMSCRGraph.mscrs, iMSCRGraph.g)

    /*
     *  Convert the Smart.DList abstract syntax tree to AST.Node abstract syntax tree.
     *  This is a side-effecting expression. The @m@ field of the @ci@ parameter is updated.
     */
    ds.foreach(_.convert(ci))     // Step 3 (see top of Intermediate.scala)
    val mscrGraph = MSCRGraph(ci) // Step 4 (See top of Intermediate.scala)

    Executor.executePlan(mscrGraph)
  }
}
