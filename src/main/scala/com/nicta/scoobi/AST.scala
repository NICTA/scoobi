/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/* Execution plan intermediate representation. */
object AST {

  object Id extends UniqueInt
  object RollingInt extends UniqueInt

  /** Intermediate representation - closer aligned to actual MSCR contetns. */
  sealed abstract class Node[A] {
    val id = Id.get
  }


  /** Input channel mapper that is not hooked up to a GBK. */
  case class Mapper[A : Manifest : HadoopWritable,
                    B : Manifest : HadoopWritable]
      (in: Node[A],
       f: A => Iterable[B])
    extends Node[B] with MapperLike[A, Int, B] {

    def mkTaggedMapper(tag: Int) = new TaggedMapper[A, Int, B](tag) {
      /* The output key will be an integer that is continually incrementing. This will ensure
       * the mapper produces an even distribution of key values. */
      def map(value: A): Iterable[(Int, B)] = f(value).map((x: B) => (RollingInt.get, x))
    }

    override def toString = "Mapper" + id
  }


  /** Input channel mapper that is hooked up to a GBK. */
  case class GbkMapper[A : Manifest : HadoopWritable,
                       K : Manifest : HadoopWritable : Ordering,
                       V : Manifest : HadoopWritable]
      (in: Node[A],
       f: A => Iterable[(K, V)])
    extends Node[(K, V)] with MapperLike[A, K, V] {

    /** */
    def mkTaggedMapper(tag: Int) = new TaggedMapper[A, K, V](tag) {
      def map(value: A): Iterable[(K, V)] = f(value)
    }

    override def toString = "GbkMapper" + id
  }


  /** Combiner. */
  case class Combiner[K, V]
      (in: Node[(K, Iterable[V])],
       f: (V, V) => V)
      (implicit mK:  Manifest[K], wtK: HadoopWritable[K], ordK: Ordering[K],
                mV:  Manifest[V], wtV: HadoopWritable[V],
                mKV: Manifest[(K, V)], wtKV: HadoopWritable[(K, V)])
    extends Node[(K, V)] with CombinerLike[V] with ReducerLike[K, V, (K, V)] {

    def mkTaggedCombiner(tag: Int) = new TaggedCombiner[V](tag) {
      def combine(x: V, y: V): V = f(x, y)
    }

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, (K, V)](tag)(mK, wtK, ordK, mV, wtV, mKV, wtKV) {
      def reduce(key: K, values: Iterable[V]): Iterable[(K, V)] = {
          List((key, values.tail.foldLeft(values.head)(f)))
      }
    }

    override def toString = "Combiner" + id
  }


  /** Reducer. */
  case class Reducer[K : Manifest : HadoopWritable : Ordering,
                     V : Manifest : HadoopWritable,
                     B : Manifest : HadoopWritable]
      (in: Node[(K, Iterable[V])],
       f: ((K, Iterable[V])) => Iterable[B])
    extends Node[B] with ReducerLike[K, V, B] {

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, B](tag) {
      def reduce(key: K, values: Iterable[V]): Iterable[B] = f((key, values))
    }

    override def toString = "Reducer" + id
  }


  /** A combiner followed by a reducer */
  case class CombinerReducer[K : Manifest : HadoopWritable : Ordering,
                             V : Manifest : HadoopWritable,
                             B : Manifest : HadoopWritable]
      (in: Node[(K, Iterable[V])],
       cf: (V, V) => V,
       rf: ((K, V)) => Iterable[B])
    extends Node[B] with CombinerLike[V] with ReducerLike[K, V, B] {

    def mkTaggedCombiner(tag: Int) = new TaggedCombiner[V](tag) {
      def combine(x: V, y: V): V = cf(x, y)
    }

    def mkTaggedReducer(tag: Int) = new TaggedReducer[K, V, B](tag) {
      def reduce(key: K, values: Iterable[V]): Iterable[B] = {
        rf((key, values.tail.foldLeft(values.head)(cf)))
      }
    }

    override def toString = "CombinerReducer" + id
  }


  /** Usual Load node. */
  case class Load(val path: String) extends Node[String] {
    override def toString = "Load" + id
  }


  /** Usual Flatten node. */
  case class Flatten[A : Manifest : HadoopWritable](ins: List[Node[A]]) extends Node[A] {
    override def toString = "Flatten" + id
  }


  /** Usual GBK node. */
  case class GroupByKey[K : Manifest : HadoopWritable : Ordering,
                        V : Manifest : HadoopWritable]
      (in: Node[(K, V)])
    extends Node[(K, Iterable[V])] {

    override def toString = "GroupByKey" + id
  }
}
