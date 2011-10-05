/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import scala.io.Source


import scala.collection.mutable.{Map => MMap}

object ConvertInfo {
  def apply(outMap: Map[Smart.DList[_], Set[Smart.Persister[_]]],
            mscrs: Iterable[Intermediate.MSCR],
            g: DGraph): ConvertInfo = {
    new ConvertInfo(outMap, mscrs, g, MMap(), MMap())
  }
}

class ConvertInfo(val outMap: Map[Smart.DList[_], Set[Smart.Persister[_]]],
                  val mscrs: Iterable[Intermediate.MSCR],
                  val g: DGraph,
                  val m: MMap[Smart.DList[_], AST.Node[_]],
                  /* A map of AST nodes to BridgeStores*/
                  val bridgeStoreMap: MMap[AST.Node[_], BridgeStore]
                  ) {

  def getASTNode[A](d: Smart.DList[_]): AST.Node[A] = m.get(d) match {
     case Some(n) => n.asInstanceOf[AST.Node[A]]
     case None    => throw new RuntimeException("Node not found in map")
  }

  def getASTCombiner[A,B](d: Smart.DList[_]): AST.Combiner[A,B] = m.get(d) match {
    case Some(n) => n.asInstanceOf[AST.Combiner[A,B]]
    case None    => throw new RuntimeException("Node not found in map")
  }

  def getASTReducer[A,B,C](d: Smart.DList[_]): AST.Reducer[A,B,C] = m.get(d) match {
    case Some(n) => n.asInstanceOf[AST.Reducer[A,B,C]]
    case None    => throw new RuntimeException("Node not found in map")
  }

  def getASTGbkReducer[A,B,C](d: Smart.DList[_]): AST.GbkReducer[A,B,C] = m.get(d) match {
    case Some(n) => n.asInstanceOf[AST.GbkReducer[A,B,C]]
    case None    => throw new RuntimeException("Node not found in map")
  }

  def getASTFlatten[A](d: Smart.DList[_]): AST.Flatten[A] = m.get(d) match {
    case Some(n) => n.asInstanceOf[AST.Flatten[A]]
    case None    => throw new RuntimeException("Node not found in map")
  }

  def getASTGroupByKey[K,V](d: Smart.DList[_]): AST.GroupByKey[K,V] = m.get(d) match {
    case Some(n) => n.asInstanceOf[AST.GroupByKey[K,V]]
    case None    => throw new RuntimeException("Node not found in map")
  }


}

/** Abstract syntax of tree of primitive "language". */
object Smart {

  import com.nicta.scoobi.{Intermediate => I}

  /** GADT for distributed list computation graph. */
  sealed abstract class DList[A : Manifest : HadoopWritable] {
    def name: String

    def insert(ci: ConvertInfo, n: AST.Node[A]): AST.Node[A] = {
      ci.m += ((this, n))
      n
    }

    def dataSource(ci: ConvertInfo): DataStore with DataSource = {
      BridgeStore.getFromMMap(ci.getASTNode(this), ci.bridgeStoreMap)
    }

    def convert(ci: ConvertInfo): AST.Node[A]  = {
      val maybeN: Option[AST.Node[_]] = ci.m.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[A]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew(ci)
      }
    }

    def convertNew(ci: ConvertInfo): AST.Node[A]

    /* TODO: Investigate whether you can do this without asInstanceOf. It just feels hacky */
    def convertNew2[K : Manifest : HadoopWritable : Ordering,
                    V : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K,V)] = this.asInstanceOf[DList[(K,V)]].convertNew(ci)

    def convertFlatMap[B : Manifest : HadoopWritable](ci: ConvertInfo, fm: FlatMap[A,B]): AST.Node[B] = {
      val n: AST.Node[A] = convert(ci)
      AST.Mapper(n, fm.f)
    }

  }

  case class Load[A : Manifest : HadoopWritable]
      (loader: Loader[A])
    extends DList[A] {
    def name = "Load"

    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.Load())
    }

    override def dataSource(ci: ConvertInfo): DataStore with DataSource =
      loader.mkInputStore(ci.getASTNode(this).asInstanceOf[AST.Load[A]])

  }

  case class FlatMap[A : Manifest : HadoopWritable,
                     B : Manifest : HadoopWritable]
      (in: DList[A],
       f: A => Iterable[B])
    extends DList[B] {
    def name = "FlatMap"
    def convertNew(ci: ConvertInfo): AST.Node[B] = {
      in.convert(ci)
      in.convertFlatMap(ci, this)
    }

    override def convertNew2[K : Manifest : HadoopWritable : Ordering,
                             V : Manifest : HadoopWritable](ci: ConvertInfo): AST.Node[(K,V)] = {
      val fm: FlatMap[A,(K,V)] = this.asInstanceOf[FlatMap[A,(K,V)]]
      val n: AST.Node[A] = fm.in.convert(ci)

      if ( ci.mscrs.exists(_.containsGbkMapper(fm)) ) {
        fm.insert2(ci, new AST.GbkMapper(n,fm.f) with KVLike[K,V] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
      } else {
        fm.insert(ci, AST.Mapper(n, fm.f))
      }
    }

  }

  case class GroupByKey[K : Manifest : HadoopWritable : Ordering,
                        V : Manifest : HadoopWritable]
      (in: DList[(K, V)])
    extends DList[(K, Iterable[V])] {
    def name = "GroupByKey"

    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.GroupByKey(in.convertNew2(ci)))
    }

    override def convertFlatMap[B : Manifest : HadoopWritable]
                               (ci: ConvertInfo, fm: FlatMap[(K,Iterable[V]), B]): AST.Node[B] = {
      val n: AST.Node[(K, Iterable[V])] = convert(ci)
      if ( ci.mscrs.exists(_.containsGbkReducer(fm)) ) {
        fm.insert(ci, AST.GbkReducer(n, fm.f))
      } else {
        fm.insert(ci, AST.Mapper(n, fm.f))
      }
    }
  }

  case class Combine[K : Manifest : HadoopWritable : Ordering,
                     V : Manifest : HadoopWritable]
      (in: DList[(K, Iterable[V])],
       f: (V, V) => V)
    extends DList[(K, V)] {
    def name = "Combine"

    def convertNew(ci: ConvertInfo) = insert(ci, AST.Combiner(in.convert(ci), f))

    override def convertFlatMap[B : Manifest : HadoopWritable]
                               (ci: ConvertInfo, fm: FlatMap[(K,V), B]): AST.Node[B] = {
      val n: AST.Node[(K, V)] = convert(ci)
      if ( ci.mscrs.exists(_.containsReducer(fm)) ) {
        fm.insert(ci, AST.Reducer(n, fm.f))
      } else {
        fm.insert(ci, AST.Mapper(n, fm.f))
      }
    }
  }

  case class Flatten[A : Manifest : HadoopWritable]
      (ins: List[DList[A]])
    extends DList[A] {
    def name = "Flatten"
    def convertNew(ci: ConvertInfo) = insert(ci, AST.Flatten(ins.map(_.convert(ci))))

    override def convertNew2[K : Manifest : HadoopWritable : Ordering,
                             V : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K,V)] = {
      val d: Flatten[(K,V)] = this.asInstanceOf[Flatten[(K,V)]]
      val ns: List[AST.Node[(K,V)]] = d.ins.map(_.convertNew2[K,V](ci))
      d.insert(ci, AST.Flatten(ns))
    }

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
