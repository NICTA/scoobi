/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileAlreadyExistsException
import scala.collection.mutable.{Set => MSet, Map => MMap}

import MSCR._
import AST._


/** Object for executing a Scoobi "plan". */
object Executor {

  /** The only state required to be passed around during execution of the
    * Scoobi compute graph.
    *
    * @param computeTable Which nodes have already been computed.
    * @param refcnts Number of nodes that are still to consume the output of an MSCR. */
  private class ExecState
      (val computeTable: MSet[Node[_]],
       val refcnts: MMap[BridgeStore, Int])


  /** For each output, traverse its MSCR graph and execute MapReduce jobs. Whilst there may
    * be multiple outputs, only visist each MSCR once. */
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


    /* Initialize compute table with all input (Load) nodes. */
    val computeTable: MSet[Node[_]] = MSet.empty
    eachNode(outputs.map(_.node)) {
      case n@Load() => computeTable += n
      case _        => Unit
    }

    /* Initialize reference counts of all intermeidate data (i.e. BridgeStores). */
    val bridges: Set[BridgeStore] = mscrs flatMap (_.inputChannels) flatMap {
      case BypassInputChannel(bs@BridgeStore(_, _), _) => List(bs)
      case MapperInputChannel(bs@BridgeStore(_, _), _) => List(bs)
      case _                                           => Nil
    } toSet

    val refcnts: Map[BridgeStore, Int] =
      bridges groupBy(identity) map { case (b, bs) => (b, bs.size) } toMap


    /* Rumble over each output and execute their containing MSCR. Thread-through the
     * the execution state as it is updated. */
    val st = new ExecState(computeTable, MMap(refcnts.toSeq: _*))
    outputs.foreach { out =>
      if (!st.computeTable.contains(out.node))
        executeMSCR(mscrs, st, containingMSCR(mscrs, out.node))
    }
  }


  /** Execute an MSCR. */
  private def executeMSCR(mscrs: Set[MSCR], st: ExecState, mscr: MSCR): Unit = {

    /* Make sure all inputs have been computed - recurse into executeMSCR. */
    mscr.inputNodes.foreach { input =>
      if (!st.computeTable.contains(input))
        executeMSCR(mscrs, st, containingMSCR(mscrs, input))
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
