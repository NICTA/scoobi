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
import scala.collection.mutable.{MutableList => MList}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.NullWritable

import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.io.func.FunctionInput
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.Smart.ConvertInfo
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.MSCRGraph
import com.nicta.scoobi.impl.exec.Executor
import com.nicta.scoobi.impl.exec.MaterializeStore
import com.nicta.scoobi.impl.exec.MaterializeId
import com.nicta.scoobi.impl.util.UniqueInt
import com.nicta.scoobi.impl.rtt.ScoobiWritable


/** A list that is distributed across multiple machines. */
class DList[A : Manifest : WireFormat](private val ast: Smart.DList[A]) { self =>

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Primitive functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Apply a specified function to "chunks" of elements from the distributed list to produce
    * zero or more output elements. The resulting output elements from the many "chunks" form
    * a new distributed list. */
  def parallelDo[B] (dofn: DoFn[A, B])(implicit mB:  Manifest[B], wtB: WireFormat[B]): DList[B] =
    new DList(Smart.ParallelDo(ast, dofn, false, false))

  /** Concatenate one or more distributed lists to this distributed list. */
  def ++(ins: DList[A]*): DList[A] = new DList(Smart.Flatten(List(ast) ::: ins.map(_.ast).toList))

  /** Group the values of a distributed list with key-value elements by key. */
  def groupByKey[K, V]
      (implicit ev:   Smart.DList[A] <:< Smart.DList[(K, V)],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                grpK: Grouping[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, Iterable[V])] = new DList(Smart.GroupByKey(ast))

  /** Apply an associative function to reduce the collection of values to a single value in a
    * key-value-collection distributed list. */
  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   Smart.DList[A] <:< Smart.DList[(K,Iterable[V])],
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                grpK: Grouping[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V]): DList[(K, V)] = new DList(Smart.Combine(ast, f))

  /** Turn a distributed list into a normal, non-distributed collection that can be accessed
    * by the client. */
  def materialize: DObject[Iterable[A]] = {

    val id = MaterializeId.get
    val path = new Path(Scoobi.getWorkingDirectory(Scoobi.conf), "materialize/" + id)

    new DObject[Iterable[A]] {
      def get: Iterable[A] = new Iterable[A] {
        def iterator = {
          val fs = FileSystem.get(path.toUri, Scoobi.conf)
          val readers = fs.globStatus(new Path(path, "ch*")) map { (stat: FileStatus) =>
            new SequenceFile.Reader(fs, stat.getPath, Scoobi.conf)
          }

          val iterators = readers.toIterable map { reader =>
            new Iterator[A] {
              val key = NullWritable.get
              val value: ScoobiWritable[A] =
                Class.forName(reader.getValueClassName).newInstance.asInstanceOf[ScoobiWritable[A]]
              def next(): A = value.get
              def hasNext: Boolean = reader.next(key, value)

            } toIterable
          }

          iterators.flatten.toIterator
        }
      }

      def use: DListPersister[_] = new DListPersister(self, new MaterializeStore(id, path))
    }
  }

  /** Used in conjunction with with Grouping options, to ensure that all prior computations up
    * to the next GroupBy or GroupByKey are contained within the same MapReduce job. For expert
    * use only. */
  def groupBarrier: DList[A] = {
    val dofn = new DoFn[A, A] {
      def setup() = {}
      def process(input: A, emitter: Emitter[A]) = emitter.emit(input)
      def cleanup(emitter: Emitter[A]) = {}
    }
    new DList(Smart.ParallelDo(ast, dofn, true, false))
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derived functionality (return DLists).
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
  def groupBy[K : Manifest : WireFormat : Grouping](f: A => K): DList[(K, Iterable[A])] =
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

  /** Builds a new distributed list from this list without any duplicate elements. */
  def distinct: DList[A] = {
    import scala.collection.mutable.{Set => MSet}

    /* Cache input values that have not been seen before. And, if a value has been
     * seen (i.e. is cached), simply drop it.
     * TODO - make it an actual cache that has a fixed size and has a replacement
     * policy once it is full otherwise there is the risk of running out of memory. */
    val dropCached = new DoFn[A, (A, Int)] {
      val cache: MSet[A] = MSet.empty
      def setup() = {}
      def process(input: A, emitter: Emitter[(A, Int)]) = {
        if (!cache.contains(input)) {
          emitter.emit((input, 0))
          cache += input
        }
      }
      def cleanup(emitter: Emitter[(A, Int)]) = {}
    }

    /* A Grouping type where sorting is implemented by taking the difference between hash
      * codes of the two values. In this case, not concerned with ordering, just that the
      * same values are grouped together. This Grouping instance will provide that. */
    implicit val grouping = new Grouping[A] {
      def sortCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
    }

    parallelDo(dropCached).groupByKey.map(_._1)
  }

  /** Create a new distributed list that is keyed based on a specified function. */
  def by[K : Manifest : WireFormat](kf: A => K): DList[(K, A)] = map { x => (kf(x), x) }

  /** Create a distribued list containing just the keys of a key-value distributed list. */
  def keys[K, V]
      (implicit ev:   A <:< (K, V),
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V])
    : DList[K] = map(ev(_)._1)

  /** Create a distribued list containing just the values of a key-value distributed list. */
  def values[K, V]
      (implicit ev:   A <:< (K, V),
                mK:   Manifest[K],
                wtK:  WireFormat[K],
                mV:   Manifest[V],
                wtV:  WireFormat[V])
    : DList[V] = map(ev(_)._2)


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derrived functionality (reduction operations)
  // TODO - should eventually return DObjects, not DLists.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Reduce the elemets of this distributed list using the specified associative binary operator. The
    * order in which the elements are reduced is unspecified and may be nondeterministic. */
  def reduce(op: (A, A) => A): DList[A] = {
    /* First, perform in-mapper combining. */
    val imc: DList[A] = self.parallelDo(new DoFn[A, A] {
      var acc: A = _
      var first = true
      def setup() = {}
      def process(input: A, emitter: Emitter[A]) =
        { if (first) { acc = input; first = false } else { acc = op(acc, input) }}
      def cleanup(emitter: Emitter[A]) = emitter.emit(acc)
    })

    /* Group all elements together (so they go to the same reducer task) and then
     * combine them. */
    imc.groupBy(_ => 0).combine(op).map(_._2)
  }

  /** Multiply up the elements of this distribute list. */
  def product(implicit num: Numeric[A]): DList[A] = reduce(num.times)

  /** Sum up the elements of this distribute list. */
  def sum(implicit num: Numeric[A]): DList[A] = reduce(num.plus)

  /** The length of the distributed list. */
  def length: DList[Int] = map(_ => 1).sum

  /** The size of the distributed list. */
  def size: DList[Int] = length

  /** Count the number of elements in the list which satisfy a predicate. */
  def count(p: A => Boolean): DList[Int] = filter(p).length

  /** Find the largest element in the distributed list. */
  def max(implicit cmp: Ordering[A]): DList[A] = reduce((x, y) => if (cmp.gteq(x, y)) x else y)

  /** Find the largest element in the distributed list. */
  def maxBy[B](f: A => B)(cmp: Ordering[B]): DList[A] =
    reduce((x, y) => if (cmp.gteq(f(x), f(y))) x else y)

  /** Find the smallest element in the distributed list. */
  def min(implicit cmp: Ordering[A]): DList[A] = reduce((x, y) => if (cmp.lteq(x, y)) x else y)

  /** Find the smallest element in the distributed list. */
  def minBy[B](f: A => B)(cmp: Ordering[B]): DList[A] =
    reduce((x, y) => if (cmp.lteq(f(x), f(y))) x else y)
}


/** This object provides a set of operations to create distributed lists. */
object DList {

  /** Creates a distributed list with given elements. */
  def apply[A : Manifest : WireFormat](elems: A*): DList[A] = {
    val vec = Vector(elems: _*)
    FunctionInput.fromFunction(vec.size)(ix => vec(ix))
  }

  /** Creates a distributed list from a data source. */
  def fromSource[K, V, A : Manifest : WireFormat](source: DataSource[K, V, A]): DList[A] =
    new DList(Smart.Load(source))

  /** Concatenates all arguement distributed lists into a single distributed list. */
  def concat[A : Manifest : WireFormat](xss: List[DList[A]]): DList[A] = new DList(Smart.Flatten(xss.map(_.ast)))

  /** Concatenates all arguement distributed lists into a single distributed list. */
  def concat[A : Manifest : WireFormat](xss: DList[A]*): DList[A] = concat(xss: _*)

  /** Creates a distributed list containing values of a given function over a range of
    * integer values starting from 0. */
  def tabulate[A : Manifest : WireFormat](n: Int)(f: Int => A): DList[A] =
    FunctionInput.fromFunction(n)(f)

  /* Pimping from generic collection types (i.e. Seq) to a Distributed List */
  trait PimpWrapper[A] { def toDList: DList[A] }
  implicit def travPimp[A : Manifest : WireFormat](trav: Traversable[A]) = new PimpWrapper[A] {
    def toDList: DList[A] = FunctionInput.fromFunction(trav.size)(trav.toSeq)
  }

  /** Persist one or more distributed lists - will trigger MapReduce computations. */
  def persist(outputs: DListPersister[_]*) = {

    /* Produce map of all unique outputs and their corresponding persisters. */
    val rawOutMap: Map[Smart.DList[_], Set[DataSink[_,_,_]]] = {
      val emptyM: Map[Smart.DList[_], Set[DataSink[_,_,_]]] = Map.empty
      outputs.foldLeft(emptyM)((m, p) => m + ((p.dlist.ast, m.getOrElse(p.dlist.ast, Set.empty) + p.sink)))
    }

    /* Optimise the plan associated with the outpus. */
    val outMap : Map[Smart.DList[_], Set[DataSink[_,_,_]]] = {
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
