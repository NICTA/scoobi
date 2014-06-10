/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package impl
package plan
package comp

import core._
import WireFormat._
import mapreducer._
import java.util.UUID._
import CollectFunctions._
import ScoobiConfigurationImpl._
import scalaz.{syntax}
import syntax.std.option._
/**
 * Processing node in the computation graph.
 *
 * It has a unique id, a BridgeStore for its outputs and some possible additional sinks.
 */
trait ProcessNodeImpl extends ProcessNode {
  // this needs to be a val because a lazy val might not be serialised properly sometimes
  // and get re-initialised to a different value! Of course this doesn't play well with setting tags in Channels.
  val id: Int = UniqueId.get

  /** unique identifier for the bridgeStore storing data for this node */
  def bridgeStoreId: String
  /**
   * ParallelDo, Combine, GroupByKey have a Bridge = sink for previous computations + source for other computations
   */
  lazy val bridgeStore = if (nodeSinks.isEmpty) createBridgeStore else oneSinkAsBridge
  /** transform one sink into a Bridge if possible */
  private lazy val oneSinkAsBridge: Bridge =
    nodeSinks.collect { case bs: BridgeStore[_] => bs }.headOption
      .orElse(nodeSinks.collect { case s: Sink with SinkSource => s }.headOption.map(s => Bridge.create(s.toSource, s, bridgeStoreId)))
      .getOrElse(createBridgeStore)

  /** create a new bridgeStore if necessary */
  private def createBridgeStore = BridgeStore(bridgeStoreId, wf)

  /** @return all the additional sinks + the bridgeStore */
  lazy val sinks = (nodeSinks :+ bridgeStore).groupBy(_.id).map(_._2.headOption).flatten.toSeq
  /** list of additional sinks for this node */
  def nodeSinks : Seq[Sink]

  /** display the bridge id */
  def bridgeToString = "(bridge " + bridgeStoreId.takeRight(5).mkString + bridgeStore.checkpointPath.fold("")(" " + _)+")"
  /** display the sinks if any */
  def nodeSinksString = if (nodeSinks.nonEmpty) nodeSinks.map(s => s.outputPath(ScoobiConfiguration())).mkString("[sinks: ", ",", "]") else ""
}

/**
 * Value node to either load or materialise a value
 */
trait ValueNodeImpl extends ValueNode with WithEnvironment {
  lazy val id: Int = UniqueId.get

  /** display the sinks if any */
  def sinksString = if (sinks.nonEmpty) sinks.map(_.stringId).mkString("[sinks: ", ",", "]") else ""
}

/**
 * The ParallelDo node type specifies the building of a CompNode as a result of applying a function to
 * all elements of an existing CompNode and concatenating the results
 */
case class ParallelDo(ins:             Seq[CompNode],
                      env:             ValueNode,
                      dofn:            DoFunction,
                      wfa:             WireReaderWriter,
                      wfb:             WireReaderWriter,
                      nodeSinks:       Seq[Sink] = Seq(),
                      bridgeStoreId:   String = randomUUID.toString) extends ProcessNodeImpl {

  def wf = wfb
  def wfe = env.wf
  override val toString = "ParallelDo ("+id+")[" + Seq(wfa, wfb, env.wf).mkString(",") + "] " +
                          bridgeToString+" "+nodeSinksString

  lazy val source = ins.collect(isALoad).headOption

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(nodeSinks = f(nodeSinks))

  /** Use this ParallelDo as a Mapper */
  def map(environment: Any, value: Any, emitter: EmitterWriter) {
    dofn.processFunction(environment, value, emitter)
  }

  /** Use this ParallelDo as a Reducer */
  /** setup this parallel do computation */
  def setup(environment: Any)(implicit sc: ScoobiConfiguration) {
    dofn.setupFunction(environment)
  }
  /** reduce key and values */
  def reduce(environment: Any, key: Any, values: Any, emitter: EmitterWriter)(implicit sc: ScoobiConfiguration) {
    map(environment, (key, values), emitter)
  }
  /** cleanup */
  def cleanup(environment: Any, emitter: EmitterWriter)(implicit sc: ScoobiConfiguration) {
    dofn.cleanupFunction(environment, emitter)
  }

  /** @return the environment object stored within the env node */
  def environment(implicit sc: ScoobiConfiguration) = env.environment(sc).pull(sc.configuration)

  /** push a computed result to the distributed cache for the parallelDo environment */
  def pushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
    env.pushEnv(result)(sc)
  }
}

object ParallelDo {
  /**
   * Fuse 2 consecutive parallelDo nodes together
   *
   * pd1 ---> pd2
   */
  private[scoobi]
  def fuse(pd1: ParallelDo, pd2: ParallelDo): ParallelDo = {
    /** Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
    def fuseDoFunction(f: DoFunction, g: DoFunction): DoFunction = new DoFunction {
      /** fusion of the setup functions */
      def setupFunction(env: Any) {
        val (env1, env2) = env match { case (e1, e2) => (e1, e2); case e => (e, e) }
        f.setupFunction(env1); g.setupFunction(env2)
      }
      /** fusion of the process functions */
      def processFunction(env: Any, input: Any, emitter: EmitterWriter) {
        val (env1, env2) = env match { case (e1, e2) => (e1, e2); case e => (e, e) }
        f.processFunction(env1, input, new EmitterWriter with DelegatedScoobiJobContext {
          def write(value: Any) { g.processFunction(env2, value, emitter) }
          def delegate = emitter
        })
      }
      /** fusion of the cleanup functions */
      def cleanupFunction(env: Any, emitter: EmitterWriter) {
        val (env1, env2) = env match { case (e1, e2) => (e1, e2); case e => (e, e) }
        f.cleanupFunction(env1, new EmitterWriter with DelegatedScoobiJobContext {
          def write(value: Any) { g.processFunction(env2, value, emitter) }
          def delegate = emitter
        })
        g.cleanupFunction(env2, emitter)
      }
    }

    /** Fusion of the environments as a pairing Operation */
    def fuseEnv(fExp: CompNode, gExp: CompNode): ValueNode =
      (fExp, gExp) match {
        case (Return1(a), Return1(b)) => Return((a, b), pair(pd1.wfe, pd2.wfe))
        case _                        => Op(fExp, gExp, (f: Any, g: Any) => (f, g), pair(pd1.wfe, pd2.wfe))
      }


    // create a new ParallelDo node fusing functions and environments
    new ParallelDo(pd1.ins, fuseEnv(pd1.env, pd2.env), fuseDoFunction(pd1.dofn, pd2.dofn),
                   pd1.wfa, pd2.wfb,
                   pd2.nodeSinks,
                   pd2.bridgeStoreId)
  }

  private[scoobi]
  def create(ins: CompNode*)(wf: WireReaderWriter): ParallelDo =
    new ParallelDo(ins.toVector, UnitDObject.newInstance.getComp, EmitterDoFunction, wf, wf)

  def create(ins: Seq[CompNode], env: ValueNode, dofn: DoFunction, wfa: WireReaderWriter, wfb: WireReaderWriter): ParallelDo =
    new ParallelDo(ins.toVector, env, dofn, wfa, wfb)

  def create(ins: Seq[CompNode], env: ValueNode, dofn: DoFunction, wfa: WireReaderWriter, wfb: WireReaderWriter, bridgeStoreId: String): ParallelDo =
    new ParallelDo(ins.toVector, env, dofn, wfa, wfb, Seq(), bridgeStoreId)

  def create(ins: Seq[CompNode], env: ValueNode, dofn: DoFunction, wfa: WireReaderWriter, wfb: WireReaderWriter, nodeSinks: Seq[Sink], bridgeStoreId: String): ParallelDo =
    new ParallelDo(ins.toVector, env, dofn, wfa, wfb, nodeSinks, bridgeStoreId)

}

object ParallelDo1 {
  /** extract only the incoming node of this parallel do */
  def unapply(node: ParallelDo): Option[Seq[CompNode]] = Some(node.ins)
}

/**
 * The Combine node type specifies the building of a CompNode as a result of applying an associative
 * function to the values of an existing key-values CompNode
 */
case class Combine(in: CompNode, dofn: DoFunction,
                   wfk:   WireReaderWriter,
                   wfv:   WireReaderWriter,
                   wfu:   WireReaderWriter,
                   nodeSinks:     Seq[Sink] = Seq(),
                   bridgeStoreId: String = randomUUID.toString) extends ProcessNodeImpl {

  def wf = pair(wfk, wfu)
  override val toString = "Combine ("+id+")["+Seq(wfk, wfu).mkString(",")+"] "+bridgeToString+" "+nodeSinksString

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(nodeSinks = f(nodeSinks))

  def reduce(values: Iterable[_], context: InputOutputContext): Option[Any] = {
    val vectorWriter = new VectorEmitterWriter(context)
    combine(values, vectorWriter)
    vectorWriter.result.headOption
  }

  private def asEmitter(emitter: EmitterWriter) = new Emitter[Any] with DelegatedScoobiJobContext {
    def emit(x: Any) { emitter.write(x) }
    def delegate = emitter
  }

  def combine(values: Iterable[Any], emitter: EmitterWriter) =
    dofn.processFunction((), values, asEmitter(emitter))

  /**
   * @return a ParallelDo node where the mapping uses the combine function to combine the Iterable[V] values
   */
  def toParallelDo = {
    val dofn = BasicDoFunction((env: Any, input: Any, emitter: EmitterWriter) => input match {
      case (key, values: Iterable[_]) => reduce(values, emitter.context).foreach(reduced => emitter.write((key, reduced)))
    })
    // Return(()) is used as the Environment because there's no need for a specific value here
    ParallelDo.create(Seq(in), Return.unit, dofn, pair(wfk, iterable(wfv)), pair(wfk, wfu), nodeSinks, bridgeStoreId)
  }
}
object Combine {
  def reducer[A](f: (A, A) => A) = DoFn((vs: Iterable[A], emitter: Emitter[A]) =>
    emitter.write(vs.reduce(f)))
}
object Combine1 {
  def unapply(node: Combine): Option[CompNode] = Some(node.in)
}

/**
 * The GroupByKey node type specifies the building of a CompNode as a result of partitioning an exiting
 * key-value CompNode by key
 */
case class GroupByKey(in: CompNode, wfk: WireReaderWriter, gpk: KeyGrouping, wfv: WireReaderWriter,
                      nodeSinks: Seq[Sink] = Seq(), bridgeStoreId: String = randomUUID.toString) extends ProcessNodeImpl {

  def wf = pair(wfk, iterable(wfv))
  override val toString = "GroupByKey ("+id+")["+Seq(wfk, wfv).mkString(",")+"] "+bridgeToString+" "+nodeSinksString

  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(nodeSinks = f(nodeSinks))
}
object GroupByKey1 {
  def unapply(gbk: GroupByKey): Option[CompNode] = Some(gbk.in)
}

/**
 * The Load node type specifies the creation of a CompNode from some source other than another CompNode.
 * A DataSource object specifies how the loading is performed
 */
case class Load(source: Source, wf: WireReaderWriter, sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  override val toString = "Load ("+id+")["+wf+"] ("+source+") "+sinksString
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}
object Load1 {
  def unapply(load: Load): Option[Source] = Some(load.source)
}

/** The Return node type specifies the building of a Exp CompNode from an "ordinary" value. */
case class Return(rt: ReturnedValue, wf: WireReaderWriter, sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  def in = rt.value
  override val toString = "Return ("+id+")["+wf+"] "+sinksString
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}

case class ReturnSC(in: ScoobiConfiguration => Seq[Any], wf: WireReaderWriter = UnitFmt, sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  override val toString = "ReturnSC ("+id+")"
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = this
}
object ReturnSC1 {
  def unapply(rt: ReturnSC): Option[ScoobiConfiguration => Seq[Any]] = Some(rt.in)
}

class ReturnedValue(val value: Any)
object Return1 {
  def unapply(rt: Return): Option[Any] = Some(rt.in)
}
object Return {
  def apply(a: Any, wf: WireReaderWriter, sinks: Seq[Sink]): Return = a match {
    case rt: ReturnedValue => new Return(rt, wf, sinks)
    case _                 => new Return(new ReturnedValue(a), wf, sinks)
  }
  def apply(a: Any, wf: WireReaderWriter): Return = a match {
    case rt: ReturnedValue => new Return(rt, wf)
    case _                 => new Return(new ReturnedValue(a), wf)
  }

  /** this must not be a lazy val shared by all graphs because it breaks concurrent test execution */
  def unit = Return((), wireFormat[Unit])

  def isUnit(node: ValueNode) = node match {
    case Return(a, _, _) => a.value == ()
    case _               => false
  }
}

case class Materialise(in: ProcessNode, wf: WireReaderWriter, sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  override val toString = "Materialise ("+id+")["+wf+"] "+sinksString
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}
object Materialise1 {
  def unapply(mt: Materialise): Option[ProcessNode] = Some(mt.in)
}

/**
 * The Op node type specifies the building of Exp CompNode by applying a function to the values
 * of two other CompNode nodes
 */
case class Op(in1: CompNode, in2: CompNode, f: (Any, Any) => Any, wf: WireReaderWriter, sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  override val toString = "Op ("+id+")["+wf+"] "+sinksString
  def execute(a: Any, b: Any): Any = f(a, b)
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}
object Op1 {
  def unapply(op: Op): Option[(CompNode, CompNode)] = Some((op.in1, op.in2))
}

case class Root(ins: Seq[CompNode], sinks: Seq[Sink] = Seq()) extends ValueNodeImpl {
  lazy val wf: WireReaderWriter = wireFormat[Unit]
  def updateSinks(f: Seq[Sink] => Seq[Sink]) = copy(sinks = f(sinks))
}

object Root1 {
  def unapply(root: Root): Option[Seq[CompNode]] = Some(root.ins)
}
/**
 * Value nodes have environments which are determined by the job configuration
 * because they are effectively files which are distributed via the distributed cache
 */
trait WithEnvironment {
  def wf: WireReaderWriter
  private var _environment: Option[Environment] = None

  def environment(sc: ScoobiConfiguration): Environment = {
    _environment getOrElse {
      val e = sc.newEnv(wf)
      _environment = Some(e)
      e
    }
  }

  /** push a value for this environment. This serialises the value and distribute it in the file cache */
  def pushEnv(result: Any)(implicit sc: ScoobiConfiguration) {
    environment(sc).push(result)(sc.configuration)
  }
}


