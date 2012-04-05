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
package com.nicta.scoobi.impl.plan

import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.Grouping
import com.nicta.scoobi.DoFn
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.impl.exec.TaggedMapper
import com.nicta.scoobi.impl.exec.TaggedCombiner
import com.nicta.scoobi.impl.exec.TaggedReducer
import com.nicta.scoobi.impl.exec.MapperLike
import com.nicta.scoobi.impl.exec.CombinerLike
import com.nicta.scoobi.impl.exec.ReducerLike
import com.nicta.scoobi.impl.exec.TaggedIdentityReducer
import com.nicta.scoobi.impl.util.UniqueInt


/* Execution plan intermediate representation. */
object AST {

  object Id extends UniqueInt
  object RollingInt extends UniqueInt

  /** Intermediate representation - closer aligned to actual MSCR contents. */
  sealed abstract class Node[A : Manifest : WireFormat] {
    val id = Id.get

    def mkStraightTaggedIdentityMapper(tags: Set[Int]): TaggedMapper[A, Int, A] =
      new TaggedMapper[A, Int, A](tags) {
        override def setup() = {}
        override def map(input: A, emitter: Emitter[(Int, A)]) = emitter.emit((RollingInt.get, input))
        override def cleanup(emitter: Emitter[(Int, A)]) = {}
      }

    def toVerboseString: String
  }


  /** Input channel mapper that is not hooked up to a GBK. */
  case class Mapper[A : Manifest : WireFormat,
                    B : Manifest : WireFormat]
      (in: Node[A],
       dofn: DoFn[A, B])
    extends Node[B] with MapperLike[A, Int, B] with ReducerLike[Int, B, B] {

    def mkTaggedMapper(tags: Set[Int]) = new TaggedMapper[A, Int, B](tags) {
      /* The output key will be an integer that is continually incrementing. This will ensure
       * the mapper produces an even distribution of key values. */
      def setup() = dofn.setup()

      def map(input: A, emitter: Emitter[(Int, B)]) = {
        val e = new Emitter[B] { def emit(x: B) = emitter.emit((RollingInt.get, x)) }
        dofn.process(input, e)
      }

      def cleanup(emitter: Emitter[(Int, B)]) = {
        val e = new Emitter[B] { def emit(x: B) = emitter.emit((RollingInt.get, x)) }
        dofn.cleanup(e)
      }
    }

    def mkTaggedReducer(tag: Int): TaggedReducer[Int, B, B] = new TaggedIdentityReducer(tag)

    override def toString = "Mapper" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Input channel mapper that is hooked up to a GBK. */
  case class GbkMapper[A : Manifest : WireFormat,
                       K : Manifest : WireFormat : Grouping,
                       V : Manifest : WireFormat]
      (in: Node[A],
       dofn: DoFn[A, (K, V)])
    extends Node[(K, V)] with MapperLike[A, K, V] with ReducerLike[K, V, (K, V)] {

    def mkTaggedMapper(tags: Set[Int]) = new TaggedMapper[A, K, V](tags) {
      def setup() = dofn.setup()
      def map(input: A, emitter: Emitter[(K, V)]) = dofn.process(input, emitter)
      def cleanup(emitter: Emitter[(K, V)]) = dofn.cleanup(emitter)
    }

    def mkTaggedReducer(tag: Int): TaggedReducer[K, V, (K, V)] =
      new TaggedReducer(tag)(implicitly[Manifest[K]], implicitly[WireFormat[K]], implicitly[Grouping[K]],
                             implicitly[Manifest[V]], implicitly[WireFormat[V]],
                             implicitly[Manifest[(K,V)]], implicitly[WireFormat[(K,V)]]) {
        def reduce(key: K, values: Iterable[V], emitter: Emitter[(K, V)]) = values.foreach { (v: V) => emitter.emit((key, v)) }
    }

    override def toString = "GbkMapper" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Combiner. */
  case class Combiner[K, V]
      (in: Node[(K, Iterable[V])],
       f: (V, V) => V)
      (implicit mK:  Manifest[K], wtK: WireFormat[K], grpK: Grouping[K],
                mV:  Manifest[V], wtV: WireFormat[V],
                mKV: Manifest[(K, V)], wtKV: WireFormat[(K, V)])
    extends Node[(K, V)] with CombinerLike[V] with ReducerLike[K, V, (K, V)] {

    def mkTaggedCombiner(tag: Int) = new TaggedCombiner[V](tag) {
      def combine(x: V, y: V): V = f(x, y)
    }

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, (K, V)](tag)(mK, wtK, grpK, mV, wtV, mKV, wtKV) {
      def reduce(key: K, values: Iterable[V], emitter: Emitter[(K, V)]) = {
        emitter.emit((key, values.reduce(f)))
      }
    }

    /** Produce a TaggedReducer using this combiner function and an additional reducer function. */
    def mkTaggedReducerWithCombiner[B](tag: Int, dofn: DoFn[(K, V), B])(implicit mB: Manifest[B], wtB: WireFormat[B]) =
      new TaggedReducer[K, V, B](tag)(mK, wtK, grpK, mV, wtV, mB, wtB) {
        def reduce(key: K, values: Iterable[V], emitter: Emitter[B]) = {
          dofn.setup()
          dofn.process((key, values.reduce(f)), emitter)
          dofn.cleanup(emitter)
        }
      }

    override def toString = "Combiner" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** GbkReducer - a reduce (i.e. ParallelDo) that follows a GroupByKey (i.e. no Combiner). */
  case class GbkReducer[K : Manifest : WireFormat : Grouping,
                        V : Manifest : WireFormat,
                        B : Manifest : WireFormat]
      (in: Node[(K, Iterable[V])],
       dofn: DoFn[((K, Iterable[V])), B])
    extends Node[B] with ReducerLike[K, V, B] {

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, B](tag) {
      def reduce(key: K, values: Iterable[V], emitter: Emitter[B]) = {
        dofn.setup()
        dofn.process((key, values), emitter)
        dofn.cleanup(emitter)
      }
    }

    override def toString = "GbkReducer" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Reducer - a reduce (i.e. FlatMap) that follows a Combiner. */
  case class Reducer[K : Manifest : WireFormat : Grouping,
                     V : Manifest : WireFormat,
                     B : Manifest : WireFormat]
      (in: Node[(K, V)],
       dofn: DoFn[(K, V), B])
    extends Node[B] with ReducerLike[K, V, B] {

    /* It is expected that this Reducer is preceded by a Combiner. */
    def mkTaggedReducer(tag: Int) = in match {
      case c@Combiner(_, _) => c.mkTaggedReducerWithCombiner(tag, dofn)
      case _                => sys.error("Reducer must be preceeded by Combiner")
    }

    override def toString = "Reducer" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Usual Load node. */
  case class Load[A : Manifest : WireFormat]() extends Node[A] {
    override def toString = "Load" + id

    def toVerboseString = toString
  }


  /** Usual Flatten node. */
  case class Flatten[A : Manifest : WireFormat](ins: List[Node[A]]) extends Node[A] with ReducerLike[Int, A, A] {
    override def toString = "Flatten" + id

    def mkTaggedReducer(tag: Int): TaggedReducer[Int, A, A] = new TaggedIdentityReducer(tag)

    def toVerboseString = toString + "(" + ins.map(_.toVerboseString).mkString("[", ",", "]") + ")"
  }


  /** Usual GBK node. */
  case class GroupByKey[K : Manifest : WireFormat : Grouping,
                        V : Manifest : WireFormat]
      (in: Node[(K, V)])
    extends Node[(K, Iterable[V])] with ReducerLike[K, V, (K, Iterable[V])] {

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, (K, Iterable[V])](tag) {
      def reduce(key: K, values: Iterable[V], emitter: Emitter[(K, Iterable[V])]) = {
        import scala.collection.immutable.VectorBuilder
        val b = new scala.collection.immutable.VectorBuilder[V]
        values foreach { b += _ }
        emitter.emit((key, b.result().toIterable))
      }
    }

    override def toString = "GroupByKey" + id

    def toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Apply a function to each node of the AST (visit each node only once). */
  def eachNode[U](starts: Set[Node[_]])(f: Node[_] => U): Unit = {
    starts foreach { visitOnce(_, f, Set()) }

    def visitOnce(node: Node[_], f: Node[_] => U, visited: Set[Node[_]]): Unit = {
      if (!visited.contains(node)) {
        node match {
          case Mapper(n, _)     => visitOnce(n, f, visited + node); f(node)
          case GbkMapper(n, _)  => visitOnce(n, f, visited + node); f(node)
          case Combiner(n, _)   => visitOnce(n, f, visited + node); f(node)
          case GbkReducer(n, _) => visitOnce(n, f, visited + node); f(node)
          case Reducer(n, _)    => visitOnce(n, f, visited + node); f(node)
          case GroupByKey(n)    => visitOnce(n, f, visited + node); f(node)
          case Flatten(ns)      => ns.foreach{visitOnce(_, f, visited + node)}; f(node)
          case Load()           => f(node)
        }
      }
    }
  }
}
