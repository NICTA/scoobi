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
package core

import impl.plan._
import io.func.FunctionInput
import io.DataSource
import io.seq.SeqInput

/**
 * A list that is distributed across multiple machines.
 *
 * It supports a few Traversable-like methods:
 *
 * - parallelDo: a 'map' operation transforming elements of the list in parallel
 * - ++: to concatenate 2 DLists
 * - groupByKey: to group a list of (key, value) elements by key, so as to get (key, values)
 * - combine: a parallel 'reduce' operation
 * - materialise: transforms a distributed list into a non-distributed list
 */
trait DList[A] {

  /**Manifest typeclass constraint. */
  implicit def m: Manifest[A]

  /**WireFormat typeclass constraint. */
  implicit def wf: WireFormat[A]

  private[scoobi]
  def getComp: Smart.DComp[A, Arr]


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Primitive functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**Apply a specified function to "chunks" of elements from the distributed list to produce
   * zero or more output elements. The resulting output elements from the many "chunks" form
   * a new distributed list. */
  def parallelDo[B: Manifest : WireFormat, E: Manifest : WireFormat](env: DObject[E], dofn: EnvDoFn[A, B, E]): DList[B]

  /**Concatenate one or more distributed lists to this distributed list. */
  def ++(ins: DList[A]*): DList[A]

  /**Group the values of a distributed list with key-value elements by key. */
  def groupByKey[K, V]
  (implicit ev: Smart.DComp[A, Arr] <:< Smart.DComp[(K, V), Arr],
   mK: Manifest[K],
   wtK: WireFormat[K],
   grpK: Grouping[K],
   mV: Manifest[V],
   wtV: WireFormat[V]): DList[(K, Iterable[V])]

  /**Apply an associative function to reduce the collection of values to a single value in a
   * key-value-collection distributed list. */
  def combine[K, V]
  (f: (V, V) => V)
  (implicit ev: Smart.DComp[A, Arr] <:< Smart.DComp[(K, Iterable[V]), Arr],
   mK: Manifest[K],
   wtK: WireFormat[K],
   grpK: Grouping[K],
   mV: Manifest[V],
   wtV: WireFormat[V]): DList[(K, V)]

  /**Turn a distributed list into a normal, non-distributed collection that can be accessed
   * by the client. */
  def materialise: DObject[Iterable[A]]

  /**Mark that all DList preceeding transformations up to the first groupByKey must be within
   * the same Map-Reduce job. */
  def groupBarrier: DList[A]

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derived functionality (return DLists).
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def parallelDo[B: Manifest : WireFormat](dofn: DoFn[A, B]): DList[B] = parallelDo(UnitDObject, dofn)

  private def basicParallelDo[B: Manifest : WireFormat](proc: (A, Emitter[B]) => Unit): DList[B] = {
    val dofn = new BasicDoFn[A, B] {
      def process(input: A, emitter: Emitter[B]) {
        proc(input, emitter)
      }
    }
    parallelDo(dofn)
  }
  
   /** Group the values of a distributed list with key-value elements by key. And explicitly
       take the grouping that should be used. This is best used when you're doing things like
       secondary sorts, or groupings with strange logic (like making sure None's / nulls are
       sprayed across all reducers.. */
  def groupByKeyWith[K, V](grpK: Grouping[K])(implicit ev: Smart.DComp[A, Arr] <:< Smart.DComp[(K, V), Arr],
    mK: Manifest[K],
    wtK: WireFormat[K],
    mV: Manifest[V],
    wtV: WireFormat[V]): DList[(K, Iterable[V])] = {
    implicit def grping = grpK
    groupByKey
  }

  /**For each element of the distributed list produce zero or more elements by
   * applying a specified function. The resulting collection of elements form a
   * new distributed list. */
  def flatMap[B: Manifest : WireFormat](f: A => Iterable[B]): DList[B] =
    basicParallelDo((input: A, emitter: Emitter[B]) => f(input).foreach {
      emitter.emit(_)
    })

  /**For each element of the distributed list produce a new element by applying a
   * specified function. The resulting collection of elements form a new
   * distributed list. */
  def map[B: Manifest : WireFormat](f: A => B): DList[B] =
    basicParallelDo((input: A, emitter: Emitter[B]) => emitter.emit(f(input)))

  /**Keep elements from the distributed list that pass a specified predicate function. */
  def filter(p: A => Boolean): DList[A] =
    basicParallelDo((input: A, emitter: Emitter[A]) => if (p(input)) {
      emitter.emit(input)
    })

  /**Keep elements from the distributed list that do not pass a specified predicate function. */
  def filterNot(p: A => Boolean): DList[A] = filter(p andThen (!_))

  /**Build a new DList by applying a partial function to all elements of this DList on
   * which the function is defined. */
  def collect[B: Manifest : WireFormat](pf: PartialFunction[A, B]): DList[B] =
    basicParallelDo((input: A, emitter: Emitter[B]) => if (pf.isDefinedAt(input)) {
      emitter.emit(pf(input))
    })

  /**Group the values of a distributed list according to some discriminator function. */
  def groupBy[K: Manifest : WireFormat : Grouping](f: A => K): DList[(K, Iterable[A])] =
    map(x => (f(x), x)).groupByKey

  /**Partitions this distributed list into a pair of distributed lists according to some
   * predicate. The first distributed list consists of elements that satisfy the predicate
   * and the second of all elements that don't. */
  def partition(p: A => Boolean): (DList[A], DList[A]) = (filter(p), filterNot(p))

  /**Converts a distributed list of iterable values into to a distributed list in which
   * all the values are concatenated. */
  def flatten[B](implicit ev: A <:< Iterable[B], mB: Manifest[B], wtB: WireFormat[B]): DList[B] =
    basicParallelDo((input: A, emitter: Emitter[B]) => input.foreach {
      emitter.emit(_)
    })

  /**Builds a new distributed list from this list without any duplicate elements. */
  def distinct: DList[A] = {
    import scala.collection.mutable.{Set => MSet}

    /* Cache input values that have not been seen before. And, if a value has been
     * seen (i.e. is cached), simply drop it.
     * TODO - make it an actual cache that has a fixed size and has a replacement
     * policy once it is full otherwise there is the risk of running out of memory. */
    val dropCached = new BasicDoFn[A, (A, Int)] {
      val cache: MSet[A] = MSet.empty

      def process(input: A, emitter: Emitter[(A, Int)]) {
        if (!cache.contains(input)) {
          emitter.emit((input, 0))
          cache += input
        }
      }
    }

    /* A Grouping type where sorting is implemented by taking the difference between hash
      * codes of the two values. In this case, not concerned with ordering, just that the
      * same values are grouped together. This Grouping instance will provide that. */
    implicit val grouping = new Grouping[A] {
      def groupCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
    }

    parallelDo(dropCached).groupByKey.map(_._1)
  }

  /**Create a new distributed list that is keyed based on a specified function. */
  def by[K: Manifest : WireFormat](kf: A => K): DList[(K, A)] = map {
    x => (kf(x), x)
  }

  /**Create a distribued list containing just the keys of a key-value distributed list. */
  def keys[K, V]
  (implicit ev: A <:< (K, V),
   mK: Manifest[K],
   wtK: WireFormat[K],
   mV: Manifest[V],
   wtV: WireFormat[V])
  : DList[K] = map(ev(_)._1)

  /**Create a distribued list containing just the values of a key-value distributed list. */
  def values[K, V]
  (implicit ev: A <:< (K, V),
   mK: Manifest[K],
   wtK: WireFormat[K],
   mV: Manifest[V],
   wtV: WireFormat[V])
  : DList[V] = map(ev(_)._2)


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derived functionality (reduction operations)
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**Reduce the elements of this distributed list using the specified associative binary operator. The
   * order in which the elements are reduced is unspecified and may be non-deterministic. */
  def reduce(op: (A, A) => A): DObject[A] = {
    /* First, perform in-mapper combining. */
    val imc: DList[A] = parallelDo(new DoFn[A, A] {
      var acc: A = _
      var first = true

      def setup() {}

      def process(input: A, emitter: Emitter[A]) = {
        if (first) {
          acc = input; first = false
        } else {
          acc = op(acc, input)
        }
      }

      def cleanup(emitter: Emitter[A]) {
        if (!first) emitter.emit(acc)
      }
    })

    /* Group all elements together (so they go to the same reducer task) and then
     * combine them. */
    val x: DObject[Iterable[A]] = imc.groupBy(_ => 0).combine(op).map(_._2).materialise
    x map {
      case it if it.isEmpty => sys.error("the reduce operation is called on an empty list")
      case it               => it.head
    }
  }

  /**Multiply up the elements of this distribute list. */
  def product(implicit num: Numeric[A]): DObject[A] = reduce(num.times)

  /**Sum up the elements of this distribute list. */
  def sum(implicit num: Numeric[A]): DObject[A] = reduce(num.plus)

  /**The length of the distributed list. */
  def length: DObject[Int] = map(_ => 1).sum

  /**The size of the distributed list. */
  def size: DObject[Int] = length

  /**Count the number of elements in the list which satisfy a predicate. */
  def count(p: A => Boolean): DObject[Int] = filter(p).length

  /**Find the largest element in the distributed list. */
  def max(implicit cmp: Ordering[A]): DObject[A] = reduce((x, y) => if (cmp.gteq(x, y)) x else y)

  /**Find the largest element in the distributed list. */
  def maxBy[B](f: A => B)(cmp: Ordering[B]): DObject[A] =
    reduce((x, y) => if (cmp.gteq(f(x), f(y))) x else y)

  /**Find the smallest element in the distributed list. */
  def min(implicit cmp: Ordering[A]): DObject[A] = reduce((x, y) => if (cmp.lteq(x, y)) x else y)

  /**Find the smallest element in the distributed list. */
  def minBy[B](f: A => B)(cmp: Ordering[B]): DObject[A] =
    reduce((x, y) => if (cmp.lteq(f(x), f(y))) x else y)
}


/** This object provides a set of operations to create distributed lists. */
object DList {

  /** Create a distributed list with given elements. */
  def apply[A : Manifest : WireFormat](elems: A*): DList[A] = SeqInput.fromSeq(elems)

  /** Create a distributed list of Ints from a Range. */
  def apply(range: Range): DList[Int] = SeqInput.fromSeq(range)

  /** Create a distributed list from a data source. */
  def fromSource[K, V, A : Manifest : WireFormat](source: DataSource[K, V, A]): DList[A] =
    new DListImpl(source)

  /** Concatenate all distributed lists into a single distributed list. */
  def concat[A : Manifest : WireFormat](xss: List[DList[A]]): DList[A] = xss match {
    case Nil      => sys.error("'concat' must take a non-empty list.")
    case x :: Nil => x
    case x :: xs  => x ++ (xs: _*)
  }

  /** Concatenate all distributed lists into a single distributed list. */
  def concat[A : Manifest : WireFormat](xss: DList[A]*): DList[A] = concat(xss.toList)

  /** Create a distributed list containing values of a given function over a range of
   * integer values starting from 0. */
  def tabulate[A : Manifest : WireFormat](n: Int)(f: Int => A): DList[A] =
    FunctionInput.fromFunction(n)(f)

  /** Create a DList with the same element repeated n times. */
  def fill[A : Manifest : WireFormat](n: Int)(a: =>A): DList[A] =
    DList(Seq.fill(n)(a):_*)

  /** Pimping from generic collection types (i.e. Seq) to a Distributed List */
  implicit def traversableToDList[A : Manifest : WireFormat](traversable: Traversable[A]) = new TraversableToDList[A](traversable)
  class TraversableToDList[A : Manifest : WireFormat](traversable: Traversable[A]) {
    def toDList: DList[A] = DList.apply(traversable.toSeq:_*)
  }
}
