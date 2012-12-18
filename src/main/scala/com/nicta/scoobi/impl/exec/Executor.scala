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
package exec

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.{Set => MSet}

import io.DataSink
import plan._
import application.ScoobiConfiguration

/** The only state required to be passed around during execution of the
  * Scoobi compute graph.
  *
  * @param computeTable Which nodes have already been computed.
  * @param refcnts Number of nodes that are still to consume the output of an MSCR. */
case class ExecState(
    conf: ScoobiConfiguration,
    computeTable: Map[AST.Node[_, _ <: Shape], Option[_]],
    refcnts: Map[BridgeStore[_], Int],
    step: Int,
    environments: Map[AST.Node[_, _ <: Shape], Env[_]],
    mscrEnvs: Set[AST.Node[_, _ <: Shape]],
    mscrs: Set[MSCR],
    matTable: Map[AST.Node[_, _ <: Shape], BridgeStore[_]]) {

  def incStep: ExecState =
    ExecState(conf, computeTable, refcnts, step + 1, environments, mscrEnvs, mscrs, matTable)

  def decRefcnt(store: BridgeStore[_]): ExecState =
    ExecState(conf, computeTable, refcnts + (store -> (refcnts(store) - 1)), step, environments, mscrEnvs, mscrs, matTable)

  def addComputedArr(node: AST.Node[_, _ <: Shape]): ExecState =
    ExecState(conf, computeTable + (node -> None), refcnts, step, environments, mscrEnvs, mscrs, matTable)

  def addComputedExp[E](node: AST.Node[_, _ <: Shape], exp: E): ExecState =
    ExecState(conf, computeTable + (node -> Some(exp)), refcnts, step, environments, mscrEnvs, mscrs, matTable)

  def isReferenced(store: BridgeStore[_]) = refcnts(store) != 0
}



/** Object for executing a Scoobi "plan". */
object Executor {
  lazy val logger = LogFactory.getLog("scoobi.Job")

  /** Prepare for execution of the graph. */
  def prepare(mscrGraph: MSCRGraph, conf: ScoobiConfiguration): ExecState = {

    val mscrs   = mscrGraph.mscrs
    val outputs = mscrGraph.outputs
    val matTable = mscrGraph.matTable
    val environments = mscrGraph.environments

    /* Initialise compute table with all input (Load) nodes. */
    val computeTable: MSet[(AST.Node[_, _ <: Shape], Option[_])] = MSet.empty
    AST.eachNode(outputs.map(_.node).toSet) {
      case n@AST.Load() => computeTable += (n -> None)
      case _            => Unit
    }

    /* Initialise reference counts of all intermediate data (i.e. BridgeStores). */
    val mscrBridges: List[BridgeStore[_]] = mscrs.toList flatMap (_.inputChannels) collect {
      case BypassInputChannel(bs@BridgeStore(), _) => bs
      case MapperInputChannel(bs@BridgeStore(), _) => bs
    }

    val refcnts: Map[BridgeStore[_], Int] =
      (mscrBridges ++ matTable.values) groupBy(identity) map { case (b, bs) => (b, bs.size) } toMap

    /* All nodes that are input to MSCRs - probably part of mscrGraph */
    val mscrEnvironments = mscrs.flatMap(_.inputEnvs)

    /* Total number of Scoobi "tasks" is the number of MSCRs, i.e. number of MR jobs. */
    logger.info("Running job: " + conf.jobId)
    logger.info("Number of steps: " + mscrs.size)


    /* Return the initial execution state. */
    ExecState(conf, computeTable.toMap, refcnts, 1, environments, mscrEnvironments, mscrs, matTable)
  }


  private def executeOnce(node: AST.Node[_, _ <: Shape], st: ExecState): ExecState = node match {
    case AST.Mapper(_, _, _)      => executeArr(node, st)
    case AST.GbkMapper(_, _, _)   => executeArr(node, st)
    case AST.Combiner(_, _)       => executeArr(node, st)
    case AST.GbkReducer(_, _, _)  => executeArr(node, st)
    case AST.Reducer(_, _, _)     => executeArr(node, st)
    case AST.GroupByKey(_)        => executeArr(node, st)
    case AST.Flatten(_)           => executeArr(node, st)
    case AST.Load()               => executeArr(node, st)
    case AST.Materialise(_)       => executeExp(node, st)._2
    case AST.Op(_, _, _)          => executeExp(node, st)._2
    case AST.Return(_)            => executeExp(node, st)._2
  }


  def executeArrOutput(node: AST.Node[_, _ <: Shape], sink: DataSink[_,_,_], st: ExecState): ExecState = node match {
    case AST.Load() => executeMSCR(node, st)
    case _          => executeArr(node, st)
  }

  private def executeArr(node: AST.Node[_, _ <: Shape], st: ExecState): ExecState =
    if (st.computeTable.contains(node)) st else executeMSCR(node, st)

  private def executeMSCR(node: AST.Node[_, _ <: Shape], st: ExecState): ExecState = {
    /* MSCR that computes this Arr node. */
    logger.debug("Executing 'MSCR'")
    var state = st
    val mscr = MSCR.containingOutput(state.mscrs, node)

    /* Make sure all inputs have been computed - recurse into executeMSCR. */
    mscr.inputNodes.foreach { n => state = executeOnce(n, state) }

    /* Make a Hadoop job and run it. */
    logger.info("Running step: " + state.step + " of " + state.mscrs.size)
    logger.info("Number of input channels: " + mscr.inputChannels.size)
    logger.info("Number of output channels: " + mscr.outputChannels.size)
    mscr.outputNodes.zipWithIndex.foreach { case (o, ix) => logger.debug(ix + ": " + o.toVerboseString) }

    MapReduceJob(state.step, mscr).run(state.conf)

    /* Update compute table - all nodes captured by this MSCR have now been "executed". */
    mscr.nodes.foreach { n => state = state.addComputedArr(n) }
    /* Remove intermediate data if possible or decrement the BridgeStores reference count */
    state = freeIntermediateOutputs(mscr, state)
    state.incStep
  }

  /* Update reference counts - decrement counts for all intermediates then
   * garbage collect any intermediates that have a zero reference count. */
  private[exec]
  def freeIntermediateOutputs(mscr: MSCR, state: ExecState): ExecState = {
    var resultingState = state
    mscr.bridgeStores.foreach { store =>
      resultingState = resultingState.decRefcnt(store)
      if (!resultingState.isReferenced(store)) { store.freePath(state.conf) }
    }
    resultingState
  }

  def executeExp[E](node: AST.Node[E, _ <: Shape], st: ExecState): (E, ExecState) = {

    /* Compute a single Exp node. */
    def operate[E](node: AST.Node[E, _ <: Shape], st: ExecState): (E, ExecState) = node match {
      case AST.Materialise(in)  => {
        logger.debug("Executing " + node)
        val st1 = executeOnce(in, st)
        val exp: Iterable[E] = st.matTable(in).asInstanceOf[BridgeStore[E]].readAsIterable(st.conf)
        (exp, st1)
      }

      case AST.Op(in1, in2, f) => {
        logger.debug("Executing " + node)
        val (exp1, st1) = executeExp(in1, st)
        val (exp2, st2) = executeExp(in2, st1)
        val exp3 = f(exp1, exp2)
        val st3 = st2.addComputedExp(node, exp3)
        (exp3, st3)
      }

      case AST.Return(x) => {
        logger.debug("Executing " + node)
        val st1 = st.addComputedExp(node, x)
        (x, st1)
      }

      case other => sys.error("not possible")
    }

    /* If already computed, return the cached value, else call the appropriate operator. */
    if (st.computeTable.contains(node)) {
      st.computeTable.get(node) match {
        case Some(Some(exp)) => (exp.asInstanceOf[E], st)
        case other           => sys.error("not possible")
      }
    } else {
      val (exp, st1) = operate(node, st)
      if (st1.mscrEnvs.contains(node)) {
        /* If this expression is an input environment to an MSCR, push its value. */
        val env: Env[E] = st1.environments(node).asInstanceOf[Env[E]]
        logger.debug("Pushing environment: " + env)
        env.push(st1.conf, exp)
      }
      (exp, st1)
    }
  }
}
