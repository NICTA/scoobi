package com.nicta.scoobi
package impl
package plan
package comp

import core._
import collection._
import util.UniqueId
import WireFormat._
import WireFormat._
import mapreducer._
import scalaz.Memo._
import scalaz.Equal
import java.util.UUID._
import CollectFunctions._
import org.apache.hadoop.conf.Configuration
import ScoobiConfigurationImpl._
/**
 * GADT for distributed list computation graph.
 */
sealed trait DComp[+A] extends CompNode {
  lazy val id = UniqueId.get

  type CompNodeType <: DComp[A]

  def wf: WireFormat[_]

  def sinks: Seq[Sink]
  def addSink(sink: Sink) = updateSinks(sinks => sinks :+ sink)
  def updateSinks(f: Seq[Sink] => Seq[Sink]): CompNodeType
  lazy val bridgeStore: Option[Bridge] = None
}

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A, B, E](
                               ins:               Seq[CompNode],
                               env:               CompNode,
                               dofn:              EnvDoFn[A, B, E],
                               implicit val wfa:  WireFormat[A],
                               implicit val wfb:  WireFormat[B],
                               implicit val wfe:  WireFormat[E],
                               sinks:             Seq[Sink] = Seq(),
                               bridgeStoreId:     String = randomUUID.toString) extends DComp[B] {

  type CompNodeType = ParallelDo[A, B, E]
  lazy val wf = wfb

  def setup(implicit configuration: Configuration) { dofn.setup(environment(ScoobiConfigurationImpl(configuration)).pull) }
  def unsafeMap[R](value: Any, emitter: Emitter[R])(implicit sc: ScoobiConfiguration) {
    val env = environment.pull(sc.configuration)
    dofn.unsafeSetup(env)
    dofn.unsafeProcess(env, value, emitter)
    dofn.unsafeCleanup(env, emitter)
  }
  def reduce[K, V](key: K, values: UntaggedValues[V], emitter: Emitter[B])(implicit configuration: Configuration) {
    dofn.process(environment(ScoobiConfigurationImpl(configuration)).pull, (key, values).asInstanceOf[A], emitter)
  }
  def cleanup(emitter: Emitter[B])(implicit configuration: Configuration) { dofn.cleanup(environment(ScoobiConfigurationImpl(configuration)).pull, emitter) }

  def environment(implicit sc: ScoobiConfiguration): Env[E] = env match {
    case e: WithEnvironment[_] => e.environment(sc).asInstanceOf[Env[E]]
    case other                 => Env[E](wfe)
  }

  def unsafePushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
    env match {
      case e: WithEnvironment[_] => e.unsafePushEnv(result)(sc)
      case other                 => ()
    }
  }

  def source = ins.collect(isALoad).headOption

  override lazy val bridgeStore = Some(BridgeStore(bridgeStoreId, wf))

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override val toString = "ParallelDo ("+id+")[" + (wfa, wfb, wfe) + "] env: " + env

  def fuse[C, F](p2: ParallelDo[_, C, F])
                (implicit wfc: WireFormat[C],
                          wff: WireFormat[F]): ParallelDo[A, C, (E, F)] =
          ParallelDo.fuse[A, C, E, F](this, p2)(wfa, wfc, wireFormat[E], wireFormat[F])

}
object ParallelDo {

  private[scoobi]
  def fuse[A : WireFormat, C : WireFormat, E : WireFormat, F : WireFormat](pd1: ParallelDo[A, _, E], pd2: ParallelDo[_, C, F]): ParallelDo[A, C, (E, F)] = {
    new ParallelDo(pd1.ins, fuseEnv[E, F](pd1.env, pd2.env), fuseDoFn(pd1.dofn.asInstanceOf[EnvDoFn[A,Any,E]], pd2.dofn.asInstanceOf[EnvDoFn[Any,C,F]]),
                   wireFormat[A], wireFormat[C], wireFormat[(E, F)],
                   pd1.sinks ++ pd2.sinks,
                   pd1.bridgeStoreId)
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
  def fuseEnv[F : WireFormat, G : WireFormat](fExp: CompNode, gExp: CompNode): DComp[(F, G)] =
    Op(fExp, gExp, (f: F, g: G) => (f, g), wireFormat[(F, G)])

}
object ParallelDo1 {
  /** extract only the incoming node of this parallel do */
  def unapply(node: ParallelDo[_,_,_]): Option[Seq[CompNode]] = Some(node.ins)
}

/** The Combine node type specifies the building of a DComp as a result of applying an associative
 * function to the values of an existing key-values DComp. */
case class Combine[K , V](in: CompNode, f: (V, V) => V,
                          implicit val wfk: WireFormat[K],
                          implicit val gpk: Grouping[K],
                          implicit val wfv: WireFormat[V],
                          sinks: Seq[Sink] = Seq(), bridgeStoreId: String = randomUUID.toString) extends DComp[(K, V)] {

  type CompNodeType = Combine[K, V]

  override lazy val bridgeStore = Some(BridgeStore(bridgeStoreId, wf))
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
  lazy val wf = wireFormat[(K, V)]
  override val toString = "Combine ("+id+")["+(wfk, wfv)+"]"

  def combine = f
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
    ParallelDo[(K, Iterable[V]), (K, V), Unit](Seq(in), Return.unit, dofn, wireFormat[(K, Iterable[V])], wireFormat[(K, V)], wireFormat[Unit])
  }

  def unsafeReduce(values: Iterable[Any]) =
    values.asInstanceOf[Iterable[V]].reduce(f)
}
object Combine1 {
  def unapply(node: Combine[_,_]): Option[CompNode] = Some(node.in)
}

/** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
 * key-value DComp by key. */
case class GroupByKey[K, V](in: CompNode,
                            implicit val wfk: WireFormat[K],
                            implicit val gpk: Grouping[K],
                            implicit val wfv: WireFormat[V], sinks: Seq[Sink] = Seq(), bridgeStoreId: String = randomUUID.toString) extends DComp[(K, Iterable[V])] {

  type CompNodeType = GroupByKey[K, V]
  lazy val wf = wireFormat[(K, Iterable[V])]
  override lazy val bridgeStore = Some(BridgeStore(bridgeStoreId, wf))

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override val toString = "GroupByKey ("+id+")["+(wfk, wfv)+"]"

}
object GroupByKey1 {
  def unapply(gbk: GroupByKey[_,_]): Option[CompNode] = Some(gbk.in)
}

/** The Load node type specifies the creation of a DComp from some source other than another DComp.
 * A DataSource object specifies how the loading is performed. */
case class Load[A](source: Source, wf: WireFormat[A], sinks: Seq[Sink] = Seq()) extends DComp[A] {

  type CompNodeType = Load[A]

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override val toString = "Load ("+id+")["+wf+"]"
}
object Load1 {
  def unapply(l: Load[_]): Option[Source] = Some(l.source)
}

/** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
case class Return[A](in: A, wf: WireFormat[A], sinks: Seq[Sink] = Seq()) extends DComp[A] with WithEnvironment[A] {

  type CompNodeType = Return[A]

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override val toString = "Return ("+id+")["+wf+"]"

}
object Return {
  def unit = Return((), wireFormat[Unit])
}
object Return1 {
  def unapply(ret: Return[_]): Option[_] = Some(ret.in)
}

/** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
case class Materialize[A](in: CompNode, wf: WireFormat[Iterable[A]], sinks: Seq[Sink] = Seq()) extends DComp[Iterable[A]] with WithEnvironment[Iterable[A]] {

  type CompNodeType = Materialize[A]

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  override val toString = "Materialize ("+id+")["+wf+"]"

}
object Materialize1 {
  def unapply(mat: Materialize[_]): Option[CompNode] = Some(mat.in)
}

/** The Op node type specifies the building of Exp DComp by applying a function to the values
 * of two other Exp DComp nodes. */
case class Op[A, B, C](in1: CompNode, in2: CompNode, f: (A, B) => C,
                       wf: WireFormat[C], sinks: Seq[Sink] = Seq()) extends DComp[C] with WithEnvironment[C] {

  type CompNodeType = Op[A, B, C]

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  def unsafeExecute(a: Any, b: Any): C = {
    val result = f(a.asInstanceOf[A], b.asInstanceOf[B])
    result
  }

  override val toString = "Op ("+id+")["+wf+"]"
}
object Op1 {
  def unapply(op: Op[_,_,_]): Option[(CompNode, CompNode)] = Some((op.in1, op.in2))
}

case class Root(ins: Seq[CompNode]) extends CompNode {
  val id = UniqueId.get
  lazy val sinks = Seq()
  lazy val bridgeStore = None
  def wf: WireFormat[_] = wireFormat[Unit]
}

trait WithEnvironment[E] {
  def wf: WireFormat[_]
  private var _environment: Option[Env[_]] = None

  def environment(sc: ScoobiConfiguration): Env[E] = {
    _environment match {
      case Some(e) => e
      case None    => val e = Env(wf)(sc); _environment = Some(e); e
    }
  }.asInstanceOf[Env[E]]

  def unsafePushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
      environment(sc).push(result.asInstanceOf[E])(sc.conf)
  }
}


