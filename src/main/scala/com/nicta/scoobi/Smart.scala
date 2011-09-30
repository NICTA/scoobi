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
      (loader: Loader[A])
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

  /** A Loader class specifies how a distributed list is materialised. */
  abstract class Loader[A : Manifest : HadoopWritable] {
    def mkInputStore(node: AST.Load[A]): InputStore
  }

  /** A Persister class specifies how a distributed list is persisted. */
  abstract class Persister[A] {
    def mkOutputStore(node: AST.Node[A]): OutputStore
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


  def parentsOf(d: DList[_]): List[DList[_]] = {
    d match {
      case Load(_)        => List()
      case FlatMap(in,_)  => List(in)
      case GroupByKey(in) => List(in)
      case Combine(in,_)  => List(in)
      case Flatten(ins)   => ins
    }
  }

  def getFlatMap(d: DList[_]): Option[FlatMap[_,_]] = {
    d match {
      case flatMap@FlatMap(_,_) => Some(flatMap)
      case _                    => None
    }
  }

  def getFlatten(d: DList[_]): Option[Flatten[_]] = {
    d match {
      case flatten@Flatten(_) => Some(flatten)
      case _                  => None
    }
  }

  def getGroupByKey(d: DList[_]): Option[GroupByKey[_,_]] = {
    d match {
      case gbk@GroupByKey(_) => Some(gbk)
      case _                 => None
    }
  }

  def getCombine(d: DList[_]): Option[Combine[_,_]] = {
    d match {
      case c@Combine(_,_) => Some(c)
      case _              => None
    }
  }

  def isFlatMap(d: DList[_]):    Boolean = getFlatMap(d).isDefined
  def isFlatten(d: DList[_]):    Boolean = getFlatten(d).isDefined
  def isGroupByKey(d: DList[_]): Boolean = getGroupByKey(d).isDefined
  def isCombine(d: DList[_]):    Boolean = getCombine(d).isDefined

}
