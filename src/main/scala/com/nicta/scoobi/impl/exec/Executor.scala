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

import java.io.IOException
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileAlreadyExistsException
import scala.collection.mutable.{Set => MSet, Map => MMap}

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.MapperInputChannel
import com.nicta.scoobi.impl.plan.StraightInputChannel
import com.nicta.scoobi.impl.plan.BypassInputChannel
import com.nicta.scoobi.impl.plan.MSCR
import com.nicta.scoobi.impl.plan.MSCRGraph


/** Object for executing a Scoobi "plan". */
object Executor {

  /** The only state required to be passed around during execution of the
    * Scoobi compute graph.
    *
    * @param computeTable Which nodes have already been computed.
    * @param refcnts Number of nodes that are still to consume the output of an MSCR. */
  private class ExecState
      (val computeTable: MSet[AST.Node[_]],
       val refcnts: MMap[BridgeStore, Int])


  /** For each output, traverse its MSCR graph and execute MapReduce jobs. Whilst there may
    * be multiple outputs, only visit each MSCR once. */
  def executePlan(mscrGraph: MSCRGraph): Unit = {

    val mscrs   = mscrGraph.mscrs
    val outputs = mscrGraph.outputStores

    /* Check that all output dirs don't already exist. */
    def pathExists(p: Path) = {
      val s = FileSystem.get(Scoobi.conf).listStatus(p)
      if (s == null)          false
      else if (s.length == 0) false
      else                    true
    }

    outputs map (_.outputPath) find (pathExists(_)) match {
      case Some(p) => throw new FileAlreadyExistsException("Output " + p + " already exists.")
      case None    => Unit
    }

    /* Check that all input dirs already exist. */
    mscrs flatMap { _.inputChannels } flatMap {
      case BypassInputChannel(input, _) => List(input)
      case MapperInputChannel(input, _) => List(input)
      case StraightInputChannel(input, _) => List(input)
    } filter {
      case BridgeStore(_, _) => false
      case _                 => true
    } map { _.inputPath } find (!pathExists(_)) match {
      case Some(p) => throw new IOException("Input " + p + " does not exist.")
      case None    => Unit
    }


    /* Initialize compute table with all input (Load) nodes. */
    val computeTable: MSet[AST.Node[_]] = MSet.empty
    AST.eachNode(outputs.map(_.node)) {
      case n@AST.Load() => computeTable += n
      case _            => Unit
    }

    /* Initialize reference counts of all intermediate data (i.e. BridgeStores). */
    val bridges: List[BridgeStore] = mscrs.toList flatMap (_.inputChannels) flatMap {
      case BypassInputChannel(bs@BridgeStore(_, _), _) => List(bs)
      case MapperInputChannel(bs@BridgeStore(_, _), _) => List(bs)
      case _                                           => Nil
    }

    val refcnts: Map[BridgeStore, Int] =
      bridges groupBy(identity) map { case (b, bs) => (b, bs.size) } toMap


    /* Rumble over each output and execute their containing MSCR. Thread-through the
     * the execution state as it is updated. */
    val st = new ExecState(computeTable, MMap(refcnts.toSeq: _*))
    outputs.foreach { out =>
      if (!st.computeTable.contains(out.node))
        executeMSCR(mscrs, st, MSCR.containingOutput(mscrs, out.node))
    }
  }


  /** Execute an MSCR. */
  private def executeMSCR(mscrs: Set[MSCR], st: ExecState, mscr: MSCR): Unit = {

    /* Make sure all inputs have been computed - recurse into executeMSCR. */
    mscr.inputNodes.foreach { input =>
      if (!st.computeTable.contains(input))
        executeMSCR(mscrs, st, MSCR.containingOutput(mscrs, input))
    }

    /* Make a Hadoop job and run it. */
    val job = MapReduceJob(mscr)
    job.run()

    /* Update compute table - all MSCR output nodes have now been produced. */
    mscr.outputNodes.foreach { node => st.computeTable += node }

    /* Update reference counts - decrement counts for all intermediates then
     * garbage collect any intermediates that have a zero reference count. */
    mscr.inputChannels.foreach { ic =>
      def updateRefcnt(store: BridgeStore) = {
        val rc = st.refcnts(store) - 1
        st.refcnts += (store-> rc)
        if (rc == 0)
          store.freePath
      }

      ic match {
        case BypassInputChannel(bs@BridgeStore(_, _), _) => updateRefcnt(bs)
        case MapperInputChannel(bs@BridgeStore(_, _), _) => updateRefcnt(bs)
        case _                                           => Unit
      }
    }
  }
}
