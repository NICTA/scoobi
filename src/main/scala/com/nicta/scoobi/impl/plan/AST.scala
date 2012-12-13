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
import exec._
import util.UniqueInt

/* Execution plan intermediate representation. */
object AST {

  object Id extends UniqueInt
  object RollingInt extends UniqueInt

  /** Intermediate representation - closer aligned to actual MSCR contents. */
  sealed abstract class Node[A : Manifest : WireFormat, Sh <: Shape] {
    val id = Id.get

    def mkStraightTaggedIdentityMapper(tags: Set[Int]): TaggedMapper[A, Unit, Int, A] =
      new TaggedMapper[A, Unit, Int, A](tags) {
        override def setup(env: Unit) = {}
        override def map(env: Unit, input: A, emitter: Emitter[(Int, A)]) = emitter.emit((RollingInt.get, input))
        override def cleanup(env: Unit, emitter: Emitter[(Int, A)]) = {}
      }

    def toVerboseString: String

    /* We don't want structural equality */
    override def equals(arg0: Any): Boolean = eq(arg0.asInstanceOf[AnyRef])
    override def hashCode = id
  }


  /** Input channel mapper that is not hooked up to a GBK. */
  case class Mapper[A : Manifest : WireFormat,
                    B : Manifest : WireFormat,
                    E : Manifest : WireFormat]
      (in: Node[A, Arr],
       env: Node[E, Exp],
       dofn: EnvDoFn[A, B, E])
    extends Node[B, Arr] with MapperLike[A, E, Int, B] with ReducerLike[Int, B, B, Unit] {

    def mkTaggedMapper(tags: Set[Int]) = new TaggedMapper[A, E, Int, B](tags) {
      /* The output key will be an integer that is continually incrementing. This will ensure
       * the mapper produces an even distribution of key values. */
      def setup(env: E) = dofn.setup(env)

      def map(env: E, input: A, emitter: Emitter[(Int, B)]) = {
        val e = new Emitter[B] { def emit(x: B) = emitter.emit((RollingInt.get, x)) }
        dofn.process(env, input, e)
      }

      def cleanup(env: E, emitter: Emitter[(Int, B)]) = {
        val e = new Emitter[B] { def emit(x: B) = emitter.emit((RollingInt.get, x)) }
        dofn.cleanup(env, e)
      }
    }

    def mkTaggedReducer(tag: Int): TaggedReducer[Int, B, B, Unit] = new TaggedIdentityReducer(tag)

    override def toString = "Mapper" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** Input channel mapper that is hooked up to a GBK. */
  case class GbkMapper[A : Manifest : WireFormat,
                       K : Manifest : WireFormat : Grouping,
                       V : Manifest : WireFormat,
                       E : Manifest : WireFormat]
      (in: Node[A, Arr],
       env: Node[E, Exp],
       dofn: EnvDoFn[A, (K, V), E])
    extends Node[(K, V), Arr] with MapperLike[A, E, K, V] with ReducerLike[K, V, (K, V), Unit] {

    def mkTaggedMapper(tags: Set[Int]) = new TaggedMapper[A, E, K, V](tags) {
      def setup(env: E) = dofn.setup(env)
      def map(env: E, input: A, emitter: Emitter[(K, V)]) = dofn.process(env, input, emitter)
      def cleanup(env: E, emitter: Emitter[(K, V)]) = dofn.cleanup(env, emitter)
    }

    def mkTaggedReducer(tag: Int): TaggedReducer[K, V, (K, V), Unit] =
      new TaggedReducer(tag)(implicitly[Manifest[K]], implicitly[WireFormat[K]], implicitly[Grouping[K]],
                             implicitly[Manifest[V]], implicitly[WireFormat[V]],
                             implicitly[Manifest[(K,V)]], implicitly[WireFormat[(K,V)]],
                             implicitly[Manifest[Unit]], implicitly[WireFormat[Unit]]) {
        def setup(env: Unit) {}
        def reduce(env: Unit, key: K, values: Iterable[V], emitter: Emitter[(K, V)]) {
          values.foreach { (v: V) => emitter.emit((key, v)) }
        }
        def cleanup(env: Unit, emitter: Emitter[(K, V)]) {}
    }

    override def toString = "GbkMapper" + id

    lazy val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"
  }


  /** Combiner. */
  case class Combiner[K, V]
      (in: Node[(K, Iterable[V]), Arr],
       f: (V, V) => V)
      (implicit mK:  Manifest[K], wtK: WireFormat[K], grpK: Grouping[K],
                mV:  Manifest[V], wtV: WireFormat[V],
                mKV: Manifest[(K, V)], wtKV: WireFormat[(K, V)])
    extends Node[(K, V), Arr] with CombinerLike[V] with ReducerLike[K, V, (K, V), Unit] {

    def mkTaggedCombiner(tag: Int) = new TaggedCombiner[V](tag) {
      def combine(x: V, y: V): V = f(x, y)
    }

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, (K, V), Unit](tag)(mK, wtK, grpK, mV, wtV, mKV, wtKV, implicitly[Manifest[Unit]], implicitly[WireFormat[Unit]]) {
      def setup(env: Unit) = {}
      def reduce(env: Unit, key: K, values: Iterable[V], emitter: Emitter[(K, V)]) = {
        emitter.emit((key, values.reduce(f)))
      }
      def cleanup(env: Unit, emitter: Emitter[(K, V)]) {}
    }

    /** Produce a TaggedReducer using this combiner function and an additional reducer function. */
    def mkTaggedReducerWithCombiner[B, E]
        (tag: Int, dofn: EnvDoFn[(K, V), B, E])
        (implicit mB: Manifest[B], wtB: WireFormat[B], mE: Manifest[E], wtE: WireFormat[E]) =
      new TaggedReducer[K, V, B, E](tag)(mK, wtK, grpK, mV, wtV, mB, wtB, mE, wtE) {
        def setup(env: E) = dofn.setup(env)
        def reduce(env: E, key: K, values: Iterable[V], emitter: Emitter[B]) = {
          dofn.setup(env)
          dofn.process(env, (key, values.reduce(f)), emitter)
        }
        def cleanup(env: E, emitter: Emitter[B]) = dofn.cleanup(env, emitter)
      }

    override def toString = "Combiner" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** GbkReducer - a reduce (i.e. ParallelDo) that follows a GroupByKey (i.e. no Combiner). */
  case class GbkReducer[K : Manifest : WireFormat : Grouping,
                        V : Manifest : WireFormat,
                        B : Manifest : WireFormat,
                        E : Manifest : WireFormat]
      (in: Node[(K, Iterable[V]), Arr],
       env: Node[E, Exp],
       dofn: EnvDoFn[((K, Iterable[V])), B, E])
    extends Node[B, Arr] with ReducerLike[K, V, B, E] {

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, B, E](tag) {
      def setup(env: E) = dofn.setup(env)
      def reduce(env: E, key: K, values: Iterable[V], emitter: Emitter[B]) = {
        dofn.setup(env)
        dofn.process(env, (key, values), emitter)
      }
      def cleanup(env: E, emitter: Emitter[B]) = dofn.cleanup(env, emitter)
    }

    override def toString = "GbkReducer" + id

    lazy val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"
  }


  /** Reducer - a reduce (i.e. FlatMap) that follows a Combiner. */
  case class Reducer[K : Manifest : WireFormat : Grouping,
                     V : Manifest : WireFormat,
                     B : Manifest : WireFormat,
                     E : Manifest : WireFormat]
      (in: Node[(K, V), Arr],
       env: Node[E, Exp],
       dofn: EnvDoFn[(K, V), B, E])
    extends Node[B, Arr] with ReducerLike[K, V, B, E] {

    /* It is expected that this Reducer is preceded by a Combiner. */
    def mkTaggedReducer(tag: Int) = in match {
      case c@Combiner(_, _) => c.mkTaggedReducerWithCombiner(tag, dofn)
      case _                => sys.error("Reducer must be preceeded by Combiner")
    }

    override def toString = "Reducer" + id

    lazy val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"
  }


  /** Usual Load node. */
  case class Load[A : Manifest : WireFormat]() extends Node[A, Arr] with ReducerLike[Int, A, A, Unit] {
    override def toString = "Load" + id

    def mkTaggedReducer(tag: Int): TaggedReducer[Int, A, A, Unit] = new TaggedIdentityReducer(tag)
    lazy val toVerboseString = toString
  }


  /** Usual Flatten node. */
  case class Flatten[A : Manifest : WireFormat](ins: List[Node[A, Arr]]) extends Node[A, Arr] with ReducerLike[Int, A, A, Unit] {
    override def toString = "Flatten" + id

    def mkTaggedReducer(tag: Int): TaggedReducer[Int, A, A, Unit] = new TaggedIdentityReducer(tag)

    lazy val toVerboseString = toString + "(" + ins.map(_.toVerboseString).mkString("[", ",", "]") + ")"
  }


  /** Usual GBK node. */
  case class GroupByKey[K : Manifest : WireFormat : Grouping,
                        V : Manifest : WireFormat]
      (in: Node[(K, V), Arr])
    extends Node[(K, Iterable[V]), Arr] with ReducerLike[K, V, (K, Iterable[V]), Unit] {

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, (K, Iterable[V]), Unit](tag) {
      def setup(env: Unit) {}
      def reduce(env: Unit, key: K, values: Iterable[V], emitter: Emitter[(K, Iterable[V])]) {
        emitter.emit((key, values))
      }
      def cleanup(env: Unit, emitter: Emitter[(K, Iterable[V])]) {}
    }

    override def toString = "GroupByKey" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** */
  case class Materialize[A : Manifest : WireFormat](in : Node[A, Arr]) extends Node[Iterable[A], Exp] {


    override def toString = "Materialize" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"
  }


  /** */
  case class Op[A : Manifest : WireFormat,
                B : Manifest : WireFormat,
                C : Manifest : WireFormat]
      (in1: Node[A, Exp],
       in2: Node[B, Exp],
       f: (A, B) => C)
    extends Node[C, Exp] {


    override def toString = "Op" + id

    lazy val toVerboseString = toString + "[" + in1.toVerboseString + "," + in2.toVerboseString + "]"
  }


  /** */
  case class Return[A : Manifest : WireFormat](x: A) extends Node[A, Exp] {


    override def toString = "Return" + id

    /**
     * we don't represent the value contained in this Return node because it is potentially very large
     * and could trigger and OutOfMemoryError
     */
    lazy val toVerboseString = toString
  }




  /** Apply a function to each node of the AST (visit each node only once). */
  def eachNode[U](starts: Set[Node[_, _ <: Shape]])(f: Node[_, _ <: Shape] => U): Unit = {
    starts foreach { visitOnce(_, f, Set()) }

    def visitOnce(node: Node[_, _ <: Shape], f: Node[_, _ <: Shape] => U, visited: Set[Node[_, _ <: Shape]]): Unit = {
      if (!visited.contains(node)) {
        node match {
          case Mapper(n, e, _)      => visitOnce(n, f, visited + node); visitOnce(e, f, visited + node); f(node)
          case GbkMapper(n, e, _)   => visitOnce(n, f, visited + node); visitOnce(e, f, visited + node); f(node)
          case Combiner(n, _)       => visitOnce(n, f, visited + node); f(node)
          case GbkReducer(n, e, _)  => visitOnce(n, f, visited + node); visitOnce(e, f, visited + node); f(node)
          case Reducer(n, e, _)     => visitOnce(n, f, visited + node); visitOnce(e, f, visited + node); f(node)
          case GroupByKey(n)        => visitOnce(n, f, visited + node); f(node)
          case Flatten(ns)          => ns.foreach{visitOnce(_, f, visited + node)}; f(node)
          case Load()               => f(node)
          case Materialize(n)       => visitOnce(n, f, visited + node); f(node)
          case Op(n1, n2, _)        => visitOnce(n1, f, visited + node); visitOnce(n2, f, visited + node); f(node)
          case Return(_)            => f(node)
        }
      }
    }
  }
}
