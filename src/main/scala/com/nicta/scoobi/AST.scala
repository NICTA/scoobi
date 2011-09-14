/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import scala.io.Source


/** Abstract syntax of tree of primitive language. */
object AST {

  /** GADT for distributed list computation graph. */
  sealed abstract class DList[A]

  /** */
  case class Load[A : Manifest : HadoopWritable]
      (path: String, parser: String => A)
    extends DList[A]

  /** */
  case class FlatMap[A : Manifest : HadoopWritable,
                     B : Manifest : HadoopWritable]
      (in: DList[A],
       f: A => Iterable[B])
    extends DList[B]

  /** */
  case class GroupByKey[K : Manifest : HadoopWritable : Ordering,
                        V : Manifest : HadoopWritable]
      (in: DList[(K, V)])
    extends DList[(K, Iterable[V])]

  /** */
  case class Combine[K : Manifest : HadoopWritable,
                     V : Manifest : HadoopWritable]
      (in: DList[(K, Iterable[V])],
       f: (V, V) => V)
    extends DList[(K, V)]

  /** */
  case class Flatten[A : Manifest : HadoopWritable]
      (ins: List[DList[A]])
    extends DList[A]



  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Interpreter for the AST. */
  def eval[A](pc: DList[A]): List[A] = pc match {
    case Load(path, parser) => Source.fromFile(path).getLines.toList map parser
    case FlatMap(in, f)     => (eval(in)).flatMap(f)
    case GroupByKey(in)     => (eval(in)).groupBy(_._1).toList.map { case (k, vs) => (k, vs.map(_._2)) }
    case Combine(in, f)     => (eval(in)).map {
                                 case (k, Nil)      => (k, Nil)
                                 case (k, v :: Nil) => (k, v)
                                 case (k, v :: vs)  => (k, vs.foldLeft(v)(f))
                               }
    case Flatten(ins)       => ins.map(eval(_)).flatten
  }
}



