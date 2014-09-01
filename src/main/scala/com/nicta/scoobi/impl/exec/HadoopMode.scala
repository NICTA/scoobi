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
import core._
import plan.comp._
import plan.mscr._
import monitor.Loggable._
import collection.Seqs._
import scalaz.{DList => _, concurrent, syntax, std}
import syntax.id._
import syntax.traverse._
import std.list._
import concurrent.Promise
import control.Exceptions._
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.impl.io.FileSystems

/**
 * Execution of Scoobi applications using Hadoop
 *
 * The overall process consists in:
 *
 *  - optimising the computation graph
 *  - defining "layers" of independent processing nodes
 *  - creating an optimal map reduce job for each layer
 *  - executing each map reduce job in sequence
 */
private[scoobi]
case class HadoopMode(sc: ScoobiConfiguration) extends MscrsDefinition with ExecutionMode {
  implicit lazy val modeLogger = LogFactory.getLog("scoobi.HadoopMode")

  /** execute a DList, storing the results in DataSinks */
  def execute(list: DList[_]) { execute(list.getComp) }
  /** execute a DObject, reading the result from a BridgeStore */
  def execute(o: DObject[_]): Any = execute(o.getComp)
  /** execute a computation graph */
  def execute(node: CompNode): Any = prepare(node)(sc) |> executeNode

  /**
   * Prepare the execution of the graph by optimising it
   */
  override protected def prepare(node: CompNode)(implicit sc: ScoobiConfiguration) =
    optimise(super.prepare(node)).debug("Optimised nodes", prettyGraph)

  /**
   * execute a computation node and return a result if one is expected
   */
  private lazy val executeNode: CompNode => Any = {
    // execute value nodes recursively, other nodes start a "layer" execution
    attr { node =>
      val banner = s"${"="*(sc.jobId.size+37)}"

      banner.info
      s"===== START OF SCOOBI JOB '${sc.jobId}' ========".info
      (banner+"\n").info

      executeMscrs(node)
      val result =
        if (!showPlanOnly(sc)) {
          val value = getValue(node)
          saveSinks(Seq(value), node)(sc)
          value
        } else Seq()

      banner.info

      s"===== END OF SCOOBI JOB '${sc.jobId}'   ========".info
      (banner+"\n").info

      result
    }
  }

  /** create independent Map Reduce jobs from a given node and execute them one by one */
  private def executeMscrs(node: CompNode) {
    val mscrs = createMscrs(node).debug("Executing map reduce jobs", (_:Seq[Mscr]).mkString("\n"))
    if (!showPlanOnly(sc)) {
      mscrs.zipWithIndex.foreach { case (mscr, i) => executeMscr(mscr, i+1, mscrs.size) }
      setJobSuccess(mscrs)
    }
  }

  /**
   * if each hadoop job executed successfully, change the _SUCCESS_JOB file into _SUCCESS
   * in each output directory
   */
  private def setJobSuccess(mscrs: Seq[Mscr]) = {
    import FileSystems._; implicit val configuration = sc.configuration

    val outputDirectories = mscrs.flatMap(_.sinks.flatMap(_.outputPath(sc)))
    val scoobiJobIsSuccessful = outputDirectories.forall(dir => listDirectPaths(dir)(sc).exists(_.getName == "_SUCCESS_JOB"))

    if (scoobiJobIsSuccessful)
      outputDirectories.foreach { outDir =>
        moveTo(outDir).apply(new Path(outDir, "._SUCCESS_JOB.crc"), new Path("._SUCCESS.crc"))
        moveTo(outDir).apply(new Path(outDir, "_SUCCESS_JOB"), new Path("_SUCCESS"))
      }
  }

  private def executeMscr(mscr: Mscr, currentMscrNumber: Int, totalMscrsNumber: Int) =
    Execution(mscr, currentMscrNumber, totalMscrsNumber).execute

  /**
   * Execution of a Mscr
   */
  private case class Execution(mscr: Mscr, mscrNumber: Int, totalMscrs: Int) {

    def execute {
      val step = s"$mscrNumber of $totalMscrs"

      (s"===== START OF MAP REDUCE JOB $step (mscr id = ${mscr.id}) ======\n").info
      val configured = configureMscr(mscr, mscrNumber, totalMscrs)
      val executed = configured.execute
      sc.updateCounters(executed.job.getCounters)
      executed.report

      mscr.sinks.info("Map reduce job sinks: ").foreach(markSinkAsFilled)
      (s"===== END OF MAP REDUCE JOB $step (mscr id = ${mscr.id}, Scoobi job = ${sc.jobId}) ======\n").info
    }

    /** configure a Mscr */
    private def configureMscr(mscr: Mscr, mscrNumber: Int, totalMscrsNumber: Int) = {
      implicit val mscrConfiguration = sc.duplicate
      val step = s"$mscrNumber of $totalMscrsNumber"

      s"Loading input nodes for map reduce job $step".debug
      mscr.inputNodes.foreach(load)

      s"Configuring map reduce job $step".debug
      MapReduceJob(mscr, mscrNumber, totalMscrsNumber).configure
    }
  }

  protected def sinksToSave(node: CompNode): Seq[Sink] =
    node match {
      case n: ValueNode => node.sinks
      case _            => Seq[Sink]()
    }

  /** @return the content of a Bridge as an Iterable */
  private def read(bs: Bridge): Any = {
    ("reading bridge "+bs.stringId).debug
    bs.readAsIterable(sc)
  }

  /** make sure that all inputs environments are fully loaded */
  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case rt @ ReturnSC1(in)    => pushEnv(rt, in(sc))
      case rt @ Return1(in)      => pushEnv(rt, in)
      case op @ Op1(in1, in2)    => pushEnv(op, op.execute(load(in1), load(in2)))
      case mt @ Materialise1(in) => pushEnv(mt, read(in.bridgeStore))
      case other                 => ()
    }
  }

  /**
   * once a node has been computed, if it defines an environment for another node push the value in the distributed cache
   * This method is synchronised because it can be called by several threads when Mscrs are executing in parallel to load
   * input nodes. However the graph attributes are not thread-safe and a "cyclic" evaluation might happen if several
   * thread are trying to evaluate the same attributes
   */
  private def pushEnv(node: CompNode, result: Any)(implicit sc: ScoobiConfiguration) = synchronized {
    usesAsEnvironment(node).map(_.pushEnv(result))
    result
  }

  private def getValue(node: CompNode): Any =
    node match {
      case n @ Op1(a, b)        => n.execute(getValue(a), getValue(b))
      case n @ Materialise1(in) => read(in.bridgeStore)
      case n @ Return1(v)       => v
      case n @ ReturnSC1(v)     => v(sc)
      case other                => Seq()
    }

}

