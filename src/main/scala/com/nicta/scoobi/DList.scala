/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** A list that is distributed accross multiple machines. */
class DList[A](private val ast: AST.DList[A]) {

  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
  /* Primitive functionality.                                                 */
  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

  /** */
  def flatMap[B](f: A => Iterable[B]): DList[B] = new DList(AST.FlatMap(ast, f))

  /** */
  def concat(in: DList[A]): DList[A] = new DList(AST.Flatten(List(ast) ::: List(in.ast)))

  /** */
  def groupByKey[K,V]
    (implicit ev: AST.DList[A] <:< AST.DList[(K, V)]): DList[(K, Iterable[V])] =
        new DList(AST.GroupByKey(ast))

  /** */
  def combine[K,V](z: V, f: (V, V) => V)
    (implicit ev: AST.DList[A] <:< AST.DList[(K,Iterable[V])]): DList[(K, V)] =
      new DList(AST.Combine(ast, z, f))


  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
  /* Derrived functionality.                                                  */
  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

  /** */
  def map[B](f: A => B): DList[B] = flatMap(x => List(f(x)))

  /** */
  def filter(f: A => Boolean): DList[A] = flatMap(x => if (f(x)) List(x) else Nil)


  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
  /* Execution.                                                               */
  /* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

  /** Execute the distributed list computation using an interpreter. */
  def runInterp: List[A] = AST.eval(ast)

}


object DList {
  /** Create a distributed list from a regular list. */
  def load[A](xs: List[A]): DList[A] = new DList(AST.Load(xs))
}

