/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.exec

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.{Set => MSet, Map => MMap}

import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.MapperInputChannel
import com.nicta.scoobi.impl.plan.StraightInputChannel
import com.nicta.scoobi.impl.plan.BypassInputChannel
import com.nicta.scoobi.impl.plan.MSCR
import com.nicta.scoobi.impl.plan.MSCRGraph
import com.nicta.scoobi.{ScoobiConfiguration, Scoobi}
import com.nicta.scoobi.impl.plan.AST.Load


/** Object for executing a Scoobi "plan". */
object Executor {
  lazy val logger = LogFactory.getLog("scoobi.Job")

  /** The only state required to be passed around during execution of the
    * Scoobi compute graph.
    *
    * @param computeTable Which nodes have already been computed.
    * @param refcnts Number of nodes that are still to consume the output of an MSCR. */
  private class ExecState
      (val computeTable: MSet[AST.Node[_]],
       val refcnts: MMap[BridgeStore[_], Int])


  /** For each output, traverse its MSCR graph and execute MapReduce jobs. Whilst there may
    * be multiple outputs, only visit each MSCR once. */
  def executePlan(mscrGraph: MSCRGraph)(implicit configuration: ScoobiConfiguration): Unit = {

    val mscrs   = mscrGraph.mscrs
    val outputs = mscrGraph.outputs

    /* Check that all output dirs don't already exist. */
    def pathExists(p: Path) = {
      val s = FileSystem.get(p.toUri, configuration).globStatus(p)
      if (s == null)          false
      else if (s.length == 0) false
      else                    true
    }

    /* Check all input sources. */
    mscrs flatMap { _.inputChannels } map {
      case BypassInputChannel(source, _)   => source
      case MapperInputChannel(source, _)   => source
      case StraightInputChannel(source, _) => source
    } filter {
      case BridgeStore() => false
      case _             => true
    } foreach { _.inputCheck() }


    /* Check all output targets. */
    outputs.flatMap(_.sinks.toList) foreach { _.outputCheck() }

    /* Initialize compute table with all input (Load) nodes. */
    val computeTable: MSet[AST.Node[_]] = MSet.empty
    AST.eachNode(outputs.map(_.node).toSet) {
      case n @ AST.Load() => computeTable += n
      case _              => Unit
    }

    /* Initialize reference counts of all intermediate data (i.e. BridgeStores). */
    val bridges: List[BridgeStore[_]] = mscrs.toList flatMap (_.inputChannels) flatMap {
      case BypassInputChannel(bs@BridgeStore(), _) => List(bs)
      case MapperInputChannel(bs@BridgeStore(), _) => List(bs)
      case _                                       => Nil
    }

    val refcnts: Map[BridgeStore[_], Int] =
      bridges groupBy(identity) map { case (b, bs) => (b, bs.size) } toMap


    /* Total number of Scoobi "tasks" is the number of MSCRs, i.e. number of MR jobs. */
    logger.info("Running job: " + configuration.jobId)
    logger.info("Number of steps: " + mscrs.size)


    /* Rumble over each output and execute their containing MSCR. Thread-through the
     * the execution state as it is updated. */
    val st = new ExecState(computeTable, MMap(refcnts.toSeq: _*))
    var step = 1
    outputs.foreach { out =>
      if (!st.computeTable.contains(out.node) || (out.node.isInstanceOf[Load[_]] && !out.sinks.isEmpty))
        step = executeMSCR(mscrs, st, MSCR.containingOutput(mscrs, out.node), step)
    }
  }


  /** Execute an MSCR. */
  private def executeMSCR(mscrs: Set[MSCR], st: ExecState, mscr: MSCR, nextStep: Int)
                         (implicit configuration: ScoobiConfiguration): Int = {

    /* Make sure all inputs have been computed - recurse into executeMSCR. */
    var step = nextStep
    mscr.inputNodes.foreach { input =>
      if (!st.computeTable.contains(input))
        step = executeMSCR(mscrs, st, MSCR.containingOutput(mscrs, input), step)
    }


    /* Make a Hadoop job and run it. */
    logger.info("Running step: " + step + " of " + mscrs.size)
    MapReduceJob(step, mscr).run(configuration)


    /* Update compute table - all MSCR output nodes have now been produced. */
    mscr.outputNodes.foreach { node => st.computeTable += node }

    /* Update reference counts - decrement counts for all intermediates then
     * garbage collect any intermediates that have a zero reference count. */
    mscr.inputChannels.foreach { ic =>
      def updateRefcnt(store: BridgeStore[_]) = {
        val rc = st.refcnts(store) - 1
        st.refcnts += (store-> rc)
        if (rc == 0)
          store.freePath
      }

      ic match {
        case BypassInputChannel(bs@BridgeStore(), _) => updateRefcnt(bs)
        case MapperInputChannel(bs@BridgeStore(), _) => updateRefcnt(bs)
        case _                                       => Unit
      }
    }

    step + 1
  }
}
