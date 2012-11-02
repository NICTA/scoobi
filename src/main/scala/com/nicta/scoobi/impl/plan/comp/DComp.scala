package com.nicta.scoobi
package impl
package plan
package comp

import core._
import collection._
import util.UniqueId
import WireFormat._
import ManifestWireFormat._
import mapreducer._
import IdSet._
import scalaz.Memo._
/**
 * GADT for distributed list computation graph.
 */
sealed trait DComp[+A] extends CompNode {
  lazy val id = UniqueId.get

  type CompNodeType <: DComp[A]
  type Sh <: Shape

  def mr: MapReducer[_]
  def mwf: ManifestWireFormat[_] = mr.mwf
  def mf = mwf.mf
  def wf = mwf.wf

  def sinks: Seq[Sink]
  def addSink(sink: Sink) = updateSinks(sinks => sinks :+ sink)
  def updateSinks(f: Seq[Sink] => Seq[Sink]): CompNodeType
  lazy val bridgeStore = BridgeStore(mf, wf)
}

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A, B, E](in:                CompNode,
                               env:               CompNode,
                               dofn:              EnvDoFn[A, B, E],
                               mr:                DoMapReducer[A, B, E],
                               sinks:             Seq[Sink] = Seq(),
                               barriers:          Barriers = Barriers()) extends DComp[B] {

  type CompNodeType = ParallelDo[A, B, E]
  type Sh = Arr

  def mwfe = mr.mwfe
  def wfe  = mwfe.wf

  def groupBarrier = barriers.groupBarrier
  def fuseBarrier = barriers.fuseBarrier


  lazy val environment = immutableHashMapMemo((sc: ScoobiConfiguration) => Env[E](wfe)(sc))

  def unsafePushEnv(e: Any)(implicit sc: ScoobiConfiguration) {
    environment(sc).push(e.asInstanceOf[E])(sc.conf)
  }

  def source = in match {
    case Load1(s) => Some(s)
    case _        => None
  }

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override def equals(a: Any) = a match {
    case pd: ParallelDo[_,_,_] => in == pd.in && env == pd.env && barriers == pd.barriers
    case _                     => false
  }

  override val toString = "ParallelDo ("+id+")" + barriers + " env: " + env
  def fuse[C, F](p2: ParallelDo[_, C, F])
                (implicit mwfc: ManifestWireFormat[C],
                          mwff: ManifestWireFormat[F]): ParallelDo[A, C, (E, F)] =
          ParallelDo.fuse[A, C, E, F](this, p2)(mr.mwfa, mwfc, mr.mwfe, mwff)

  def makeTaggedReducer(tag: Int)                           = mr.makeTaggedReducer(tag)
  def makeTaggedMapper(gbk: GroupByKey[_,_],tags: Set[Int]) = mr.makeTaggedMapper(tags, dofn, gbk.mwfk, gbk.gpk, gbk.mwfv)
  def makeTaggedMapper(tags: Set[Int])                      = mr.makeTaggedMapper(tags, dofn, mwf)
}
case class Barriers(groupBarrier: Boolean = false, fuseBarrier: Boolean = false) {
  override def toString = (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "")
}

object ParallelDo {

  private[scoobi]
  def fuse[A, C, E, F](pd1: ParallelDo[A, _, E], pd2: ParallelDo[_, C, F])
                (implicit
                 mwfa: ManifestWireFormat[A],
                 mwfc: ManifestWireFormat[C],
                 mwfe: ManifestWireFormat[E],
                 mwff: ManifestWireFormat[F]): ParallelDo[A, C, (E, F)] = {
    new ParallelDo(pd1.in, fuseEnv[E, F](pd1.env, pd2.env), fuseDoFn(pd1.dofn.asInstanceOf[EnvDoFn[A,Any,E]], pd2.dofn.asInstanceOf[EnvDoFn[Any,C,F]]),
                   DoMapReducer(mwfa, mwfc, manifestWireFormat[(E, F)]),
                   pd1.sinks ++ pd2.sinks,
                   Barriers(pd1.groupBarrier || pd2.groupBarrier, pd2.fuseBarrier))
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
  def fuseEnv[F : ManifestWireFormat, G : ManifestWireFormat](fExp: CompNode, gExp: CompNode): DComp[(F, G)] =
    Op(fExp, gExp, (f: F, g: G) => (f, g), SimpleMapReducer(manifestWireFormat[(F, G)]))
}
object ParallelDo1 {
  /** extract only the incoming node of this parallel do */
  def unapply(pd: ParallelDo[_,_,_]): Option[CompNode] = Some(pd.in)
}


/** The Flatten node type specifies the building of a DComp that contains all the elements from
 * one or more existing DLists of the same type. */
case class Flatten[A](ins: List[CompNode], mr: SimpleMapReducer[A], sinks: Seq[Sink] = Seq()) extends DComp[A] {

  type CompNodeType = Flatten[A]
  type Sh = Arr

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override def equals(a: Any) = a match {
    case f: Flatten[_] => ins == f.ins
    case _             => false
  }

  override val toString = "Flatten ("+id+")"

  def makeTaggedReducer(tag: Int) = mr.makeTaggedReducer(tag)
}
object Flatten1 {
  /** extract only the incoming nodes of this flatten */
  def unapply(fl: Flatten[_]): Option[Seq[CompNode]] = Some(fl.ins)
}
/** The Combine node type specifies the building of a DComp as a result of applying an associative
 * function to the values of an existing key-values DComp. */
case class Combine[K, V](in: CompNode, f: (V, V) => V, mr: KeyValueMapReducer[K, V], sinks: Seq[Sink] = Seq()) extends DComp[(K, V)] {

  type CompNodeType = Combine[K, V]
  type Sh = Arr

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  def gpk = mr.gpk
  val (mwfk, mwfv) = (mr.mwfk, mr.mwfv)
  implicit val (mfk, mfv, wfk, wfv) = (mwfk.mf, mwfv.mf, mwfk.wf, mwfv.wf)

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
    ParallelDo[(K, Iterable[V]), (K, V), Unit](in, Return.unit, dofn, DoMapReducer(manifestWireFormat[(K, Iterable[V])], manifestWireFormat[(K, V)], manifestWireFormat[Unit]))
  }

  def makeTaggedCombiner(tag: Int) = mr.makeTaggedCombiner(tag, f)
  def makeTaggedReducer(tag: Int)  = mr.makeTaggedReducer(tag, f)

  def unsafeReduce(values: Iterable[Any]) =
    values.asInstanceOf[Iterable[V]].reduce(f)
}
object Combine1 {
  def unapply(combine: Combine[_,_]): Option[CompNode] = Some(combine.in)
}

/** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
 * key-value DComp by key. */
case class GroupByKey[K, V](in: CompNode, mr: KeyValuesMapReducer[K, V], sinks: Seq[Sink] = Seq()) extends DComp[(K, Iterable[V])] {

  type CompNodeType = GroupByKey[K, V]
  type Sh = Arr

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  def gpk = mr.gpk
  val (mwfk, mwfv) = (mr.mwfk, mr.mwfv)
  implicit val (mfk, mfv, wfk, wfv) = (mwfk.mf, mwfv.mf, mwfk.wf, mwfv.wf)

  override def equals(a: Any) = a match {
    case g: GroupByKey[_,_] => in == g.in
    case _                  => false
  }

  override val toString = "GroupByKey ("+id+")"

  def makeTaggedReducer(tag: Int)              = mr.makeTaggedReducer(tag)
  def makeTaggedIdentityMapper(tags: Set[Int]) = mr.makeTaggedIdentityMapper(tags)
}
object GroupByKey1 {
  def unapply(gbk: GroupByKey[_,_]): Option[CompNode] = Some(gbk.in)
}

/** The Load node type specifies the creation of a DComp from some source other than another DComp.
 * A DataSource object specifies how the loading is performed. */
case class Load[A](source: Source, mr: SimpleMapReducer[A], sinks: Seq[Sink] = Seq()) extends DComp[A] {

  type CompNodeType = Load[A]
  type Sh = Arr

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override def equals(a: Any) = a match {
    case l: Load[_] => source == l.source
    case _          => false
  }
  override val toString = "Load ("+id+")"
}
object Load1 {
  def unapply(l: Load[_]): Option[Source] = Some(l.source)
}

/** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
case class Return[A](in: A, mr: SimpleMapReducer[A], sinks: Seq[Sink] = Seq()) extends DComp[A] {

  type CompNodeType = Return[A]
  type Sh = Exp

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override def equals(a: Any) = a match {
    case r: Return[_] => in == r.in
    case _            => false
  }
  override val toString = "Return ("+id+")"

}
object Return {
  def unit = Return((), SimpleMapReducer(manifestWireFormat[Unit]))
}
object Return1 {
  def unapply(ret: Return[_]): Option[_] = Some(ret.in)
}

/** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
case class Materialize[A](in: CompNode, mr: SimpleMapReducer[A], sinks: Seq[Sink] = Seq()) extends DComp[Iterable[A]] {

  type CompNodeType = Materialize[A]
  type Sh = Exp

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

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
case class Op[A, B, C](in1: CompNode, in2: CompNode, f: (A, B) => C, mr: SimpleMapReducer[C], sinks: Seq[Sink] = Seq()) extends DComp[C] {

  type CompNodeType = Op[A, B, C]
  type Sh = Exp

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override def equals(a: Any) = a match {
    case o: Op[_,_,_] => in1 == o.in1 && in2 == o.in2
    case _            => false
  }

  def unsafeExecute(a: Any, b: Any): C = f(a.asInstanceOf[A], b.asInstanceOf[B])

  override val toString = "Op ("+id+")"
}
object Op1 {
  def unapply(op: Op[_,_,_]): Option[(CompNode, CompNode)] = Some((op.in1, op.in2))
}


