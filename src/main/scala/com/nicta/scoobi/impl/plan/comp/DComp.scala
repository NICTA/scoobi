package com.nicta.scoobi
package impl
package plan
package comp

import org.kiama.attribution.Attributable
import core._
import io.DataSource
import WireFormat._
import org.kiama.attribution.Attribution._
import exec._
import collection._
import IdSet._
import scala.collection.immutable.SortedSet
import control.Exceptions._

/**
 * GADT for distributed list computation graph.
 */
sealed trait DComp[+A, +Sh <: Shape] extends CompNode {
  def mf: Manifest[_]
  def wf: WireFormat[_]

  def makeStraightTaggedIdentityMapper(tags: Set[Int]): TaggedMapper =
    new TaggedIdentityMapper(tags: Set[Int], manifest[Int], wireFormat[Int], grouping[Int], mf, wf) {
      override def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit((RollingInt.get, input)) }
    }
}

/**
 * Base trait for "computation nodes" with no generic type information for easier rewriting
 */
trait CompNode extends Attributable {
  lazy val id = Id.get
  lazy val dataSource: DataSource[_,_,_] = BridgeStore()
}

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A, B, E](in:               CompNode,
                               env:              CompNode,
                               dofn:             EnvDoFn[A, B, E],
                               groupBarrier:     Boolean = false,
                               fuseBarrier:      Boolean = false,
                               implicit val mfa: Manifest[A],
                               implicit val wfa: WireFormat[A],
                               implicit val mfb: Manifest[B],
                               implicit val wfb: WireFormat[B],
                               implicit val mfe: Manifest[E],
                               implicit val wfe: WireFormat[E]) extends DComp[B, Arr] {
  def mf = mfb
  def wf = wfb

  override def equals(a: Any) = a match {
    case pd: ParallelDo[_,_,_] => in == pd.in && env == pd.env && groupBarrier == pd.groupBarrier && fuseBarrier == pd.fuseBarrier
    case _                     => false
  }

  override val toString = "ParallelDo ("+id+")" + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "") + " env: " + env
  def fuse[C, F](p2: ParallelDo[_, C, F])
                (implicit mfc: Manifest[C],
                          wfc: WireFormat[C],
                          mff: Manifest[F],
                          wff: WireFormat[F]): ParallelDo[A, C, (E, F)] =
          ParallelDo.fuse[A, C, E, F](this, p2)(mfa, wfa, mfc, wfc, mfe, wfe, mff, wff)

  override lazy val dataSource = in.dataSource

  def makeTaggedReducer(tag: Int)                           = dofn.makeTaggedReducer(tag, mfb, wfb)
  def makeTaggedMapper(gbk: GroupByKey[_,_],tags: Set[Int]) = dofn.makeTaggedMapper(tags, gbk.mfk, gbk.wfk, gbk.gpk, gbk.mfv, gbk.wfv)
  def makeTaggedMapper(tags: Set[Int])                      = dofn.makeTaggedMapper(tags, mfb, wfb)
}

object ParallelDo {

  private[scoobi]
  def fuse[A, C, E, F](pd1: ParallelDo[A, _, E], pd2: ParallelDo[_, C, F])
                (implicit
                 mfa: Manifest[A],
                 wfa: WireFormat[A],
                 mfc: Manifest[C],
                 wfc: WireFormat[C],
                 mfe: Manifest[E],
                 wfe: WireFormat[E],
                 mff: Manifest[F],
                 wff: WireFormat[F]): ParallelDo[A, C, (E, F)] = {
    new ParallelDo(pd1.in, fuseEnv[E, F](pd1.env, pd2.env), fuseDoFn(pd1.dofn.asInstanceOf[EnvDoFn[A,Any,E]], pd2.dofn.asInstanceOf[EnvDoFn[Any,C,F]]),
                   pd1.groupBarrier || pd2.groupBarrier,
                   pd2.fuseBarrier,
                   mfa, wfa, mfc, wfc,
                   manifest[(E, F)], wireFormat[(E, F)])
  }

  /** Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
  private[scoobi]
  def fuseDoFn[X, Z, F, G](f: EnvDoFn[X, Any, F], g: EnvDoFn[Any, Z, G]): EnvDoFn[X, Z, (F, G)] = new EnvDoFn[X, Z, (F, G)] {
    def setup(env: (F, G)) { f.setup(env._1); g.setup(env._2) }

    def process(env: (F, G), input: X, emitter: Emitter[Z]) {
      f.process(env._1, input, new Emitter[Any] { def emit(value: Any) { g.process(env._2, value, emitter) } } )
    }

    def cleanup(env: (F, G), emitter: Emitter[Z]) {
      f.cleanup(env._1, new Emitter[Any] { def emit(value: Any) { g.process(env._2, value, emitter) } })
      g.cleanup(env._2, emitter)
    }
  }

  /** Create a new environment by forming a tuple from two separate evironments.*/
  private[scoobi]
  def fuseEnv[F : Manifest : WireFormat, G : Manifest : WireFormat](fExp: CompNode, gExp: CompNode): DComp[(F, G), Exp] =
    Op(fExp, gExp, (f: F, g: G) => (f, g), manifest[(F, G)], wireFormat[(F, G)])
}
object ParallelDo1 {
  /** extract only the incoming node of this parallel do */
  def unapply(pd: ParallelDo[_, _, _]): Option[CompNode] = Some(pd.in)
}


/** The Flatten node type specifies the building of a DComp that contains all the elements from
 * one or more existing DLists of the same type. */
case class Flatten[A](ins: List[CompNode], implicit val mf: Manifest[A], wf: WireFormat[A]) extends DComp[A, Arr] {

  override def equals(a: Any) = a match {
    case f: Flatten[_] => ins == f.ins
    case _             => false
  }

  override val toString = "Flatten ("+id+")"

  def makeTaggedReducer(tag: Int) = new TaggedIdentityReducer(tag, mf, wf)
}
object Flatten1 {
  /** extract only the incoming nodes of this flatten */
  def unapply(fl: Flatten[_]): Option[Seq[CompNode]] = Some(fl.ins)
}
/** The Combine node type specifies the building of a DComp as a result of applying an associative
 * function to the values of an existing key-values DComp. */
case class Combine[K, V](in: CompNode, f: (V, V) => V,
                         implicit val mfk: Manifest[K],
                         implicit val wfk: WireFormat[K],
                         implicit val gpk: Grouping[K],
                         implicit val mfv: Manifest[V],
                         implicit val wfv: WireFormat[V]) extends DComp[(K, V), Arr] {
  def mf = manifest[(K, V)]
  def wf = wireFormat[(K, V)]

  override def equals(a: Any) = a match {
    case c: Combine[_,_] => in == c.in
    case _               => false
  }

  override val toString = "Combine ("+id+")"

  /**
   * @return a ParallelDo node where the mapping uses the combine function to combine the Iterable[V] values
   */
  def toParallelDo = {
    val dofn = new BasicDoFn[(K, Iterable[V]), (K, V)] {
      def process(input: (K, Iterable[V]), emitter: Emitter[(K, V)]) {
        val (key, values) = input
        emitter.emit((key, values.reduce(f)))
      }
    }
    // Return(()) is used as the Environment because there's no need for a specific value here
    ParallelDo(in, Return((), manifest[Unit], wireFormat[Unit]), dofn, false, false,
      manifest[(K, Iterable[V])], wireFormat[(K, Iterable[V])],
      manifest[(K, V)], wireFormat[(K, V)],
      manifest[Unit],   wireFormat[Unit])
  }

  def makeTaggedCombiner(tag: Int) = new TaggedCombiner[V](tag, mfv, wfv) {
    def combine(x: V, y: V): V = f(x, y)
  }
  def makeTaggedReducer(tag: Int) = new TaggedReducer(tag, manifest[(K, V)], wireFormat[(K, V)]) {
    def setup(env: Any) {}
    def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
      emitter.emit((key, values.reduce((v1, v2) => f(v1.asInstanceOf[V], v2.asInstanceOf[V]))))
    }
    def cleanup(env: Any, emitter: Emitter[Any]) {}
  }
}
object Combine1 {
  def unapply(combine: Combine[_, _]): Option[CompNode] = Some(combine.in)
}

/** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
 * key-value DComp by key. */
case class GroupByKey[K, V](in: CompNode,
                            implicit val mfk: Manifest[K],
                            implicit val wfk: WireFormat[K],
                            implicit val gpk: Grouping[K],
                            implicit val mfv: Manifest[V],
                            implicit val wfv: WireFormat[V]) extends DComp[(K, Iterable[V]), Arr] {

  def mf = manifest[(K, Iterable[V])]
  def wf = wireFormat[(K, Iterable[V])]

  override def equals(a: Any) = a match {
    case g: GroupByKey[_,_] => in == g.in
    case _                  => false
  }

  override val toString = "GroupByKey ("+id+")"

  def makeTaggedReducer(tag: Int) = new TaggedReducer(tag, manifest[(K, V)], wireFormat[(K, V)]) {
    def setup(env: Any) {}
    def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
      values.foreach(value => emitter.emit((key, value)))
    }
    def cleanup(env: Any, emitter: Emitter[Any]) {}
  }
  def makeTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper(tags, mfk, wfk, gpk, mfv, wfv)
}
object GroupByKey1 {
  def unapply(gbk: GroupByKey[_, _]): Option[CompNode] = Some(gbk.in)
}

/** The Load node type specifies the creation of a DComp from some source other than another DComp.
 * A DataSource object specifies how the loading is performed. */
case class Load[A](source: DataSource[_, _, A], implicit val mf: Manifest[A], implicit val wf: WireFormat[A]) extends DComp[A, Arr] {

  override def equals(a: Any) = a match {
    case l: Load[_] => source == l.source
    case _          => false
  }
  override val toString = "Load ("+id+")"
  override lazy val dataSource: DataSource[_,_,_] = source
}
object Load1 {
  def unapply(l: Load[_]): Option[DataSource[_,_,_]] = Some(l.source)
}

/** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
case class Return[A](in: A,
                     implicit val mf: Manifest[A],
                     implicit val wf: WireFormat[A]) extends DComp[A, Exp] {
  override def equals(a: Any) = a match {
    case r: Return[_] => in == r.in
    case _            => false
  }
  override val toString = "Return ("+id+")"
}
object Return {
  def unit = Return((), manifest[Unit], wireFormat[Unit])
}
object Return1 {
  def unapply(ret: Return[_]): Option[_] = Some(ret.in)
}

/** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
case class Materialize[A](in: CompNode,
                          implicit val mf: Manifest[A],
                          implicit val wf: WireFormat[A]) extends DComp[Iterable[A], Exp] {
  override def equals(a: Any) = a match {
    case mat: Materialize[_] => in == mat.in
    case _                   => false
  }
  override val toString = "Materialize ("+id+")"
}
object Materialize1 {
  def unapply(mat: Materialize[_]): Option[CompNode] = Some(mat.in)
}

/** The Op node type specifies the building of Exp DComp by applying a function to the values
 * of two other Exp DComp nodes. */
case class Op[A, B, C](in1: CompNode, in2: CompNode, f: (A, B) => C,
                       implicit val mf: Manifest[C],
                       implicit val wf: WireFormat[C]) extends DComp[C, Exp] {
  override def equals(a: Any) = a match {
    case o: Op[_,_,_] => in1 == o.in1 && in2 == o.in2
    case _            => false
  }

  override val toString = "Op ("+id+")"
}
object Op1 {
  def unapply(op: Op[_, _, _]): Option[(CompNode, CompNode)] = Some((op.in1, op.in2))
}

object CompNode extends CompNodes
trait CompNodes {
  /** @return a sequence of distinct nodes */
  def distinctNodes[T <: CompNode](nodes: Seq[Attributable]): Set[T] =
    nodes.asNodes.map(n => (n.asInstanceOf[T].id, n.asInstanceOf[T])).toMap.values.toSet

  /** @return true if a node is the ancestor of another */
  def isAncestor(n: Attributable, other: Attributable): Boolean = other != null && n != null && !(other eq n) && ((other eq n.parent) || isAncestor(n.parent, other))

  /** @return true if a node has a parent */
  def hasParent(node: CompNode) = Option(node.parent).isDefined

  /**
   * syntax enhancement to force the conversion of an Attributable node to a CompNode
   */
  implicit def asCompNode(a: Attributable): AsCompNode = AsCompNode(a)
  case class AsCompNode(a: Attributable) {
    def asNode = a.asInstanceOf[CompNode]
  }

  /**
   * syntax enhancement to force the conversion of an iterator of Attributable nodes
   * (as returned by 'childeren' for example) to a list of CompNodes
   */
  implicit def asCompNodes(as: Iterator[Attributable]): AsCompNodes = AsCompNodes(as.toSeq)
  implicit def asCompNodes(as: Seq[Attributable]): AsCompNodes = AsCompNodes(as)
  case class AsCompNodes(as: Seq[Attributable]) {
    def asNodes = as.map(_.asNode)
  }

  /** return true if a CompNodeis a Flatten */
  lazy val isFlatten: CompNode => Boolean = { case f: Flatten[_] => true; case other => false }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case p: ParallelDo[_,_,_] => true; case other => false }
  /** return true if a CompNodeis a Flatten */
  lazy val isAFlatten: PartialFunction[Any, Flatten[_]] = { case f: Flatten[_] => f }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo[_,_,_]] = { case p: ParallelDo[_,_,_] => p }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case g: GroupByKey[_,_] => true; case other => false }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey[_,_]] = { case gbk: GroupByKey[_,_] => gbk }
  /** return true if a CompNodeis a Materialize */
  lazy val isMaterialize: CompNode => Boolean = { case Materialize(_,_,_) => true; case other => false }
  /** return true if a CompNodeis a Return */
  lazy val isReturn: CompNode => Boolean = { case Return(_,_,_) => true; case other => false }
  /** return true if a CompNode is an Op */
  lazy val isOp: CompNode => Boolean = { case Op(_,_,_,_,_) => true; case other => false }
  /** return true if a CompNode has a cycle in its graph */
  lazy val isCyclic: CompNode => Boolean = (n: CompNode) => tryKo(n -> descendents)

  /** compute the inputs of a given node */
  lazy val inputs : CompNode => SortedSet[CompNode] = attr {
    case n  => n.children.asNodes.toIdSet
  }

  /**
   *  compute the outputs of a given node.
   *  They are all the parents of the node where the parent inputs contain this node.
   */
  lazy val outputs : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => (node -> parents) collect { case a if (a -> inputs).exists(_ eq node) => a }
  }

  /**
   *  compute the shared input of a given node.
   *  They are all the distinct inputs of a node which are also inputs of another node
   */
  lazy val sharedInputs : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => ((node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in })
  }

  /**
   *  compute the siblings of a given node.
   *  They are all the nodes which share at least one input with this node
   */
  lazy val siblings : CompNode => SortedSet[CompNode] = attr {
    case node: CompNode => (node -> inputs).flatMap { in => (in -> outputs) }.filterNot(_ eq node)
  }

  /** @return true if a node has siblings */
  lazy val hasSiblings : CompNode => Boolean = attr { case node: CompNode => (node -> siblings).nonEmpty }
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node */
  lazy val descendents : CompNode => SortedSet[CompNode] =
    attr { case node: CompNode => (node -> nonUniqueDescendents).toIdSet }

  private lazy val nonUniqueDescendents : CompNode => Seq[CompNode] =
    attr { case node: CompNode => (node.children.asNodes ++ node.children.asNodes.flatMap(nonUniqueDescendents)) }

  /** @return a function returning true if one node can be reached from another, i.e. it is in the list of its descendents */
  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).contains(target)
    }(n)

  /** compute the ancestors of a node, that is all the direct parents of this node up to a root of the graph */
  lazy val ancestors : CompNode => SortedSet[CompNode] =
    circular(IdSet.empty[CompNode]: SortedSet[CompNode]) {
      case node: CompNode => {
        val p = Option(node.parent).toSeq.asNodes
        (p ++ p.flatMap { parent => ancestors(parent) }).toIdSet
      }
    }

  /** compute all the parents of a given node. A node A is parent of a node B if B can be reached from A */
  lazy val parents : CompNode => SortedSet[CompNode] =
    circular(IdSet.empty[CompNode]: SortedSet[CompNode]) {
      case node: CompNode => {
        (node -> ancestors).flatMap { ancestor =>
          ((ancestor -> descendents) + ancestor).filter(canReach(node))
        }
      }
    }

  /** @return an option for the potentially missing parent of a node */
  lazy val parentOpt: CompNode => Option[CompNode] = attr { case n => Option(n.parent).map(_.asNode) }

  /** compute the vertices starting from a node */
  lazy val vertices : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => ((node +: node.children.asNodes.flatMap(n => n -> vertices).toSeq) ++ node.children.asNodes).
        toIdSet.toSeq // make the vertices unique
    }

  /** compute all the edges which compose this graph */
  lazy val edges : CompNode => Seq[(CompNode, CompNode)] =
    circular(Seq[(CompNode, CompNode)]()) {
      case node: CompNode => (node.children.asNodes.map(n => node -> n) ++ node.children.asNodes.flatMap(n => n -> edges).toSeq).
        map { case (a, b) => (a.id, b.id) -> (a, b) }.toMap.values.toSeq // make the edges unique
    }

}
