/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import scala.io.Source


/** Abstract syntax of tree of primitive "language". */
object Smart {

  /** GADT for distributed list computation graph. */
  sealed abstract class DList[A] {
    def name: String
  }

  case class Load[A : Manifest : HadoopWritable]
      (path: String, parser: String => A)
    extends DList[A] {
    def name = "Load"
  }

  case class FlatMap[A : Manifest : HadoopWritable,
                     B : Manifest : HadoopWritable]
      (in: DList[A],
       f: A => Iterable[B])
    extends DList[B] {
    def name = "FlatMap"
  }

  case class GroupByKey[K : Manifest : HadoopWritable : Ordering,
                        V : Manifest : HadoopWritable]
      (in: DList[(K, V)])
    extends DList[(K, Iterable[V])] {
    def name = "GroupByKey"
  }

  case class Combine[K : Manifest : HadoopWritable,
                     V : Manifest : HadoopWritable]
      (in: DList[(K, Iterable[V])],
       f: (V, V) => V)
    extends DList[(K, V)] {
    def name = "Combine"
  }

  case class Flatten[A : Manifest : HadoopWritable]
      (ins: List[DList[A]])
    extends DList[A] {
    def name = "Flatten"
  }



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

  def parentsOf(d: DList[_]): List[DList[_]] = {
    d match {
      case Load(_,_)      => List()
      case FlatMap(in,_)  => List(in)
      case GroupByKey(in) => List(in)
      case Combine(in,_)  => List(in)
      case Flatten(ins)   => ins
    }
  }

  def getFlatMap(d: DList[_]): Option[DList[_]] = {
    d match {
      case FlatMap(_,_) => Some(d)
      case _            => None
    }
  }

  def getFlatten(d: DList[_]): Option[DList[_]] = {
    d match {
      case Flatten(_) => Some(d)
      case _            => None
    }
  }

  def getGroupByKey(d: DList[_]): Option[DList[_]] = {
    d match {
      case GroupByKey(_) => Some(d)
      case _             => None
    }
  }

  def getCombine(d: DList[_]): Option[DList[_]] = {
    d match {
      case Combine(_,_) => Some(d)
      case _            => None
    }
  }

  def isFlatMap(d: DList[_]):    Boolean = getFlatMap(d).isDefined
  def isFlatten(d: DList[_]):    Boolean = getFlatten(d).isDefined
  def isGroupByKey(d: DList[_]): Boolean = getGroupByKey(d).isDefined
  def isCombine(d: DList[_]):    Boolean = getCombine(d).isDefined

}
