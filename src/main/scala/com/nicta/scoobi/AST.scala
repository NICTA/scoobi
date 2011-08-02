/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** Abstract syntax of tree of primitive language. */
object AST {

  abstract class DList[A]

  case class Load[A](xs: List[A]) extends DList[A]

  case class FlatMap[A, B](in: DList[A], f: A => Iterable[B]) extends DList[B]

  case class GroupByKey[K, V](in: DList[(K, V)]) extends DList[(K, Iterable[V])]

  case class Combine[K, V](in: DList[(K, Iterable[V])], z: V, f: (V, V) => V) extends DList[(K, V)]

  case class Flatten[A](ins: List[DList[A]]) extends DList[A]


  /** Interpreter for the AST. */
  def eval[A](pc: DList[A]): List[A] = pc match {
    case Load(xs)           => xs
    case FlatMap(in, f)     => (eval(in)).flatMap(f)
    case GroupByKey(in)     => (eval(in)).groupBy(_._1).toList.map { case (k, vs) => (k, vs.map(_._2)) }
    case Combine(in, z, f)  => (eval(in)).map { case (k, vs) => (k, vs.foldLeft(z)(f)) }
    case Flatten(ins)       => ins.map(eval(_)).flatten
  }
}



