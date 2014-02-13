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

/**
 * Execution of Scoobi applications using Hadoop
 *
 * The overall process consists in:
 *
 *  - optimising the computation graph
 *  - defining "layers" of independent processing nodes
 *  - defining optimal Mscrs in each layer
 *  - executing each layer in sequence
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
   * execute a computation node
   */
  private
  def executeNode: CompNode => Any = {
    /** return the result of the last layer */
    def executeLayers(node: CompNode) {
      val layers = createMapReduceLayers(node).debug("Executing layers", showLayers)
      if (!showPlanOnly(sc)) {
        val perLayerJobs = layers.map { _.mscrs.size }
        val totalJobs = perLayerJobs.sum
        val prevLayerJobs = (0 until perLayerJobs.size).map { i => perLayerJobs.slice(0, i).sum }
        assert(prevLayerJobs.size == layers.size)
        layers.zip(prevLayerJobs).foreach { case (layer, prevJobs) => executeLayer(prevJobs, totalJobs)(layer) }
      }
    }

    def showLayers = (layers: Seq[Layer]) =>
      mkStrings(layers.flatMap(l => l +: l.mscrs))

    def getValue(node: CompNode): Any = {
      node match {
        case n @ Op1(a, b)        => n.execute(getValue(a), getValue(b))
        case n @ Materialise1(in) => read(in.bridgeStore)
        case n @ Return1(v)       => v
        case n @ ReturnSC1(v)     => v(sc)
        case other                => Seq()
      }
    }
    // execute value nodes recursively, other nodes start a "layer" execution
    attr { node =>
      val banner = s"${"="*(sc.jobId.size+37)}"

      banner.info
      s"===== START OF SCOOBI JOB '${sc.jobId}' ========".info
      (banner+"\n").info

      executeLayers(node)
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

  private def executeLayer(prevLayersMscrs: Int, totalMscrsNumber: Int): Layer => Unit =
    attr { case layer =>
      Execution(layer, prevLayersMscrs, totalMscrsNumber).execute
    }

  /**
   * Execution of a "layer" of Mscrs
   */
  private case class Execution(layer: Layer, prevLayersMscrs: Int, totalMscrsNumber: Int) {

    def execute {
      (s"Executing layer ${layer.id}\n"+layer).debug
      runMscrs(layer.mscrs, prevLayersMscrs, totalMscrsNumber)

      layer.sinks.info("Layer sinks: ").foreach(markSinkAsFilled)
      ("===== END OF LAYER "+layer.id+" ======\n").info
    }

    /**
     * run mscrs concurrently if there are more than one.
     *
     * Only the execution part is done concurrently, not the configuration.
     * This is to make sure that there is not undesirable race condition during the setting up of variables
     */
    private def runMscrs(mscrs: Seq[Mscr], prevLayersMscrs: Int, totalMscrsNumber: Int) {
      ("executing map reduce jobs"+mscrs.mkString("\n", "\n", "\n")).debug

      val configured = mscrs.toList.zipWithIndex.map { case (mscr, i) =>
        configureMscr(prevLayersMscrs + i + 1, totalMscrsNumber)(mscr)
      }

      if (sc.concurrentJobs) {
        "executing the map reduce jobs concurrently".debug
        configured.map(executeMscr).sequence.get.map(reportMscr)
      } else {
        "executing the map reduce jobs sequentially".debug
        configured.map { case (job, step) => reportMscr(job.execute, step) }
      }
    }

    /** configure a Mscr */
    private def configureMscr(mscrNumber: Int, totalMscrsNumber: Int) = (mscr: Mscr) => {
      implicit val mscrConfiguration = sc.duplicate
      val step = s"$mscrNumber of $totalMscrsNumber"

      s"Loading input nodes for map reduce job $step".debug
      mscr.inputNodes.foreach(load)

      s"Configuring map reduce job $step".debug
      (MapReduceJob(mscr, layer.id, mscrNumber, totalMscrsNumber).configure, step)
    }

    /** execute a Mscr */
    protected def executeMscr = ((job: MapReduceJob, step: String) => {
      Promise(tryOr((job.execute, step))((e: Exception) => { e.printStackTrace; (job, step) }))
    }).tupled

    /** report the execution of a Mscr */
    protected def reportMscr = ((job: MapReduceJob, step: String) => {
      // update the original configuration with the job counters for each job
      sc.updateCounters(job.job.getCounters)
      job.report
      (s"===== END OF MAP-REDUCE JOB $step (for the Scoobi job '${sc.jobId}') ======\n").info
    }).tupled
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

}

