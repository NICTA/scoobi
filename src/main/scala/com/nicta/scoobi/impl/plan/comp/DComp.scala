package com.nicta.scoobi
package impl
package plan
package comp

import core._
import WireFormat._
import mapreducer._
import java.util.UUID._
import CollectFunctions._
import org.apache.hadoop.conf.Configuration
import util.UniqueId

/**
 * Processing node in the computation graph
 */
trait ProcessNode extends CompNode {
  val id: Int = UniqueId.get
  /** ParallelDo, Combine, GroupByKey have a Bridge = sink for previous computations + source for other computations */
  def bridgeStore: Bridge
  /** list of additional sinks for this node */
  def sinks : Seq[Sink]
  def addSink(sink: Sink) = updateSinks(sinks => sinks :+ sink)
  def updateSinks(f: Seq[Sink] => Seq[Sink]): ProcessNode
}

/**
 * Value node to either load or materialise a value
 */
trait ValueNode extends CompNode {
  val id: Int = UniqueId.get
}

/**
 * The ParallelDo node type specifies the building of a CompNode as a result of applying a function to
 * all elements of an existing CompNode and concatenating the results
 */
case class ParallelDo(ins:           Seq[CompNode],
                      env:           CompNode,
                      dofn:          DoFunction,
                      wfa:           WireReaderWriter,
                      wfb:           WireReaderWriter,
                      wfe:           WireReaderWriter,
                      sinks:         Seq[Sink] = Seq(),
                      bridgeStoreId: String = randomUUID.toString) extends ProcessNode {

  lazy val wf = wfb
  override val toString = "ParallelDo ("+id+")[" + Seq(wfa, wfb, wfe).mkString(",") + "] env: " + env

  def source = ins.collect(isALoad).headOption

  lazy val bridgeStore = BridgeStore(bridgeStoreId, wf)
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))


  def setup(implicit configuration: Configuration) { dofn.setupFunction(environment(ScoobiConfigurationImpl(configuration)).pull) }
  def map(value: Any, emitter: EmitterWriter)(implicit sc: ScoobiConfiguration) {
    val env = environment.pull(sc.configuration)
    dofn.setupFunction(env)
    dofn.processFunction(env, value, emitter)
    dofn.cleanupFunction(env, emitter)
  }
  def reduce(key: Any, values: Any, emitter: EmitterWriter)(implicit configuration: Configuration) {
    dofn.processFunction(environment(ScoobiConfigurationImpl(configuration)).pull, (key, values), emitter)
  }
  def cleanup(emitter: EmitterWriter)(implicit configuration: Configuration) { dofn.cleanupFunction(environment(ScoobiConfigurationImpl(configuration)).pull, emitter) }

  def environment(implicit sc: ScoobiConfiguration): Env = env match {
    case e: WithEnvironment => e.environment(sc)
    case other              => Env(wfe)
  }

  def pushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
    env match {
      case e: WithEnvironment => e.pushEnv(result)(sc)
      case other              => ()
    }
  }
}
object ParallelDo {

  private[scoobi]
  def fuse(pd1: ParallelDo, pd2: ParallelDo)
          (wfa: WireReaderWriter,
           wfb: WireReaderWriter,
           wfc: WireReaderWriter,
           wfe: WireReaderWriter,
           wff: WireReaderWriter): ParallelDo = {
    new ParallelDo(pd1.ins, fuseEnv(pd1.env, pd2.env)(wfe, wff), fuseDoFunction(pd1.dofn, pd2.dofn),
                   wfa, wfc, pair(wfe, wff),
                   pd1.sinks ++ pd2.sinks,
                   pd2.bridgeStoreId)
  }

  /** Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
  private def fuseDoFunction(f: DoFunction, g: DoFunction): DoFunction = new DoFunction {
    def setupFunction(env: Any) { env match { case (e1, e2) => f.setupFunction(e1); g.setupFunction(e2) } }

    def processFunction(env: Any, input: Any, emitter: EmitterWriter) {
      env match { case (e1, e2) =>
        f.processFunction(e1, input, new EmitterWriter { def write(value: Any) { g.processFunction(e2, value, emitter) } } )
      }
    }

    def cleanupFunction(env: Any, emitter: EmitterWriter) {
      env match { case (e1, e2) =>
        f.cleanupFunction(e1, new EmitterWriter { def write(value: Any) { g.processFunction(e2, value, emitter) } })
        g.cleanupFunction(e2, emitter)
      }
    }
  }

  /** Create a new environment by forming a tuple from two separate evironments.*/
  private def fuseEnv(fExp: CompNode, gExp: CompNode)(wff: WireReaderWriter, wfg: WireReaderWriter): CompNode =
    Op(fExp, gExp, (f: Any, g: Any) => (f, g), pair(wff, wfg))

  private[scoobi]
  def create(ins: CompNode*)(wf: WireReaderWriter) =
    ParallelDo(ins, UnitDObject.newInstance.getComp, EmitterDoFunction, wf, wf, wireFormat[Unit])

}

object ParallelDo1 {
  /** extract only the incoming node of this parallel do */
  def unapply(node: ParallelDo): Option[Seq[CompNode]] = Some(node.ins)
}

/**
 * The Combine node type specifies the building of a CompNode as a result of applying an associative
 * function to the values of an existing key-values CompNode
 */
case class Combine(in: CompNode, f: (Any, Any) => Any,
                          wfk:   WireReaderWriter,
                          gpk:   KeyGrouping,
                          wfv:   WireReaderWriter,
                          sinks:              Seq[Sink] = Seq(),
                          bridgeStoreId:      String = randomUUID.toString) extends ProcessNode {

  lazy val wf = pair(wfk, wfv)
  override val toString = "Combine ("+id+")["+Seq(wfk, wfv).mkString(",")+"]"

  lazy val bridgeStore = BridgeStore(bridgeStoreId, wf)
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))

  def combine(values: Iterable[Any]) = values.reduce(f)

  /**
   * @return a ParallelDo node where the mapping uses the combine function to combine the Iterable[V] values
   */
  def toParallelDo = {
    val dofn = BasicDoFunction((env: Any, input: Any, emitter: EmitterWriter) => input match {
      case (key, values: Seq[_]) => emitter.write((key, values.reduce(f)))
    })
    // Return(()) is used as the Environment because there's no need for a specific value here
    ParallelDo(Seq(in), Return.unit, dofn, pair(wfk, iterable(wfv)), pair(wfk, wfv), wireFormat[Unit])
  }
}
object Combine1 {
  def unapply(node: Combine): Option[CompNode] = Some(node.in)
}

/**
 * The GroupByKey node type specifies the building of a CompNode as a result of partitioning an exiting
 * key-value CompNode by key
 */
case class GroupByKey(in: CompNode, wfk: WireReaderWriter, gpk: KeyGrouping, wfv: WireReaderWriter,
                      sinks: Seq[Sink] = Seq(), bridgeStoreId: String = randomUUID.toString) extends ProcessNode {

  lazy val wf = pair(wfk, iterable(wfv))
  override val toString = "GroupByKey ("+id+")["+Seq(wfk, wfv).mkString(",")+"]"

  lazy val bridgeStore = BridgeStore(bridgeStoreId, wf)
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}
object GroupByKey1 {
  def unapply(gbk: GroupByKey): Option[CompNode] = Some(gbk.in)
}

/**
 * The Load node type specifies the creation of a CompNode from some source other than another CompNode.
 * A DataSource object specifies how the loading is performed
 */
case class Load(source: Source, wf: WireReaderWriter) extends ValueNode {
  override val toString = "Load ("+id+")["+wf+"]"
}
object Load1 {
  def unapply(load: Load): Option[Source] = Some(load.source)
}

/** The Return node type specifies the building of a Exp CompNode from an "ordinary" value. */
case class Return(in: Any, wf: WireReaderWriter) extends ValueNode with WithEnvironment {
  override val toString = "Return ("+id+")["+wf+"]"
}
object Return1 {
  def unapply(rt: Return): Option[Any] = Some(rt.in)
}
object Return {
  def unit = Return((), wireFormat[Unit])
}

case class Materialise(in: ProcessNode, wf: WireReaderWriter) extends ValueNode with WithEnvironment {
  override val toString = "Materialise ("+id+")["+wf+"]"
}
object Materialise1 {
  def unapply(mt: Materialise): Option[ProcessNode] = Some(mt.in)
}

/**
 * The Op node type specifies the building of Exp CompNode by applying a function to the values
 * of two other CompNode nodes
 */
case class Op(in1: CompNode, in2: CompNode, f: (Any, Any) => Any, wf: WireReaderWriter) extends ValueNode with WithEnvironment {
  override val toString = "Op ("+id+")["+wf+"]"
  def execute(a: Any, b: Any): Any = f(a, b)
}
object Op1 {
  def unapply(op: Op): Option[(CompNode, CompNode)] = Some((op.in1, op.in2))
}

case class Root(ins: Seq[CompNode]) extends ValueNode {
  lazy val wf: WireReaderWriter = wireFormat[Unit]
}

trait WithEnvironment {
  def wf: WireReaderWriter
  private var _environment: Option[Env] = None

  def environment(sc: ScoobiConfiguration): Env = {
    _environment match {
      case Some(e) => e
      case None    => val e = Env(wf)(sc); _environment = Some(e); e
    }
  }

  def pushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
    environment(sc).push(result)(sc.conf)
  }
}


