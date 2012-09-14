package com.nicta.scoobi
package impl
package plan
package comp

import org.kiama.attribution.Attributable
import core.{WireFormat, EnvDoFn, Emitter, BasicDoFn}
import io.DataSource

/**
 * GADT for distributed list computation graph.
 */
sealed trait DComp[+A, +Sh <: Shape] extends CompNode

/**
 * Base trait for "computation nodes" with no generic type information for easier rewriting
 */
trait CompNode extends Attributable {
  lazy val id = Id.get
}

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A : WireFormat, B : WireFormat, E : WireFormat](in: DComp[A, Arr], env: DComp[E, Exp], dofn: EnvDoFn[A, B, E], groupBarrier: Boolean = false, fuseBarrier: Boolean = false) extends DComp[B, Arr] {
  val (awf, bwf, ewf) = (implicitly[WireFormat[A]], implicitly[WireFormat[B]], implicitly[WireFormat[E]])

  override val toString = "ParallelDo ("+id+")" + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "") + " env: " + env
  def fuse[Z : WireFormat, G : WireFormat](p2: ParallelDo[B, Z, G]) = ParallelDo.fuse(this, p2)
}
object ParallelDo {
  def fuse[X : WireFormat, Y : WireFormat, Z : WireFormat, F : WireFormat, G : WireFormat](pd1: ParallelDo[X, Y, F], pd2: ParallelDo[Y, Z, G]): ParallelDo[X, Z, (F, G)] = {
    val ParallelDo(in1, env1, dofn1, gb1, _)   = pd1
    val ParallelDo(in2, env2, dofn2, gb2, fb2) = pd2
    new ParallelDo(in1, fuseEnv(env1, env2), fuseDoFn(dofn1, dofn2), gb1 || gb2, fb2)
  }

  /** Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
  def fuseDoFn[X, Y, Z, F, G](f: EnvDoFn[X, Y, F], g: EnvDoFn[Y, Z, G]): EnvDoFn[X, Z, (F, G)] = new EnvDoFn[X, Z, (F, G)] {
    def setup(env: (F, G)) { f.setup(env._1); g.setup(env._2) }

    def process(env: (F, G), input: X, emitter: Emitter[Z]) {
      f.process(env._1, input, new Emitter[Y] { def emit(value: Y) { g.process(env._2, value, emitter) } } )
    }

    def cleanup(env: (F, G), emitter: Emitter[Z]) {
      f.cleanup(env._1, new Emitter[Y] { def emit(value: Y) { g.process(env._2, value, emitter) } })
      g.cleanup(env._2, emitter)
    }
  }

  /** Create a new environment by forming a tuple from two separate evironments.*/
  def fuseEnv[F : WireFormat, G : WireFormat](fExp: DComp[F, Exp], gExp: DComp[G, Exp]): DComp[(F, G), Exp] = Op(fExp, gExp, (f: F, g: G) => (f, g))

}


/** The Flatten node type specifies the building of a DComp that contains all the elements from
 * one or more existing DLists of the same type. */
case class Flatten[A](ins: List[DComp[A, Arr]]) extends DComp[A, Arr] {
  override val toString = "Flatten ("+id+")"
}

/** The Combine node type specifies the building of a DComp as a result of applying an associative
 * function to the values of an existing key-values DComp. */
case class Combine[K : WireFormat, V : WireFormat](in: DComp[(K, Iterable[V]), Arr], f: (V, V) => V) extends DComp[(K, V), Arr] {
  val (kwf, vwf) = (implicitly[WireFormat[K]], implicitly[WireFormat[V]])

  override val toString = "Combine ("+id+")"

  /**
   * @return a ParallelDo node where the mapping uses the combine function to combine the Iterable[V] values
   */
  def toParallelDo = {
    val dofn = new BasicDoFn[(K, Iterable[V]), (K, V)] {
      def process(input: (K, Iterable[V]), emitter: Emitter[(K, V)]) {
        val (key, values) = input
        emitter.emit(key, values.reduce(f))
      }
    }
    // Return(()) is used as the Environment because there's no need for a specific value here
    ParallelDo(in, Return(()), dofn)
  }
}

/** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
 * key-value DComp by key. */
case class GroupByKey[K, V](in: DComp[(K, V), Arr]) extends DComp[(K, Iterable[V]), Arr] {
  override val toString = "GroupByKey ("+id+")"
}

/** The Load node type specifies the creation of a DComp from some source other than another DComp.
 * A DataSource object specifies how the loading is performed. */
case class Load[A](source: DataSource[_, _, A]) extends DComp[A, Arr] {
  override val toString = "Load ("+id+")"
}

/** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
case class Return[A : WireFormat](x: A) extends DComp[A, Exp] {
  val wf = implicitly[WireFormat[A]]
  override val toString = "Return ("+id+")"
}

/** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
case class Materialize[A : WireFormat](in: DComp[A, Arr]) extends DComp[Iterable[A], Exp] {
  val wf = implicitly[WireFormat[A]]
  override val toString = "Materialize ("+id+")"
}

/** The Op node type specifies the building of Exp DComp by applying a function to the values
 * of two other Exp DComp nodes. */
case class Op[A, B, C : WireFormat](in1: DComp[A, Exp], in2: DComp[B, Exp], f: (A, B) => C) extends DComp[C, Exp] {
  val wf = implicitly[WireFormat[C]]
  override val toString = "Op ("+id+")"
}

object CompNode {
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
  lazy val isFlatten: CompNode => Boolean = { case Flatten(_) => true; case other => false }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isParallelDo: CompNode => Boolean = { case ParallelDo(_,_,_,_,_) => true; case other => false }
  /** return true if a CompNodeis a Flatten */
  lazy val isAFlatten: PartialFunction[Any, Flatten[_]] = { case f @ Flatten(_) => f }
  /** return true if a CompNodeis a ParallelDo */
  lazy val isAParallelDo: PartialFunction[Any, ParallelDo[_,_,_]] = { case p @ ParallelDo(_,_,_,_,_) => p }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isGroupByKey: CompNode => Boolean = { case GroupByKey(_) => true; case other => false }
  /** return true if a CompNodeis a GroupByKey */
  lazy val isAGroupByKey: PartialFunction[Any, GroupByKey[_,_]] = { case gbk @ GroupByKey(_) => gbk }
  /** return true if a CompNodeis a Materialize */
  lazy val isMaterialize: CompNode => Boolean = { case Materialize(_) => true; case other => false }
  /** return true if a CompNodeis a Return */
  lazy val isReturn: CompNode => Boolean = { case Return(_) => true; case other => false }
  /** return true if a CompNodeis an Op */
  lazy val isOp: CompNode => Boolean = { case Op(_,_,_) => true; case other => false }

}
