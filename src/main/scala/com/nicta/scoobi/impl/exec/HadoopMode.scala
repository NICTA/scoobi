package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import core._
import plan.comp._
import plan.mscr._
import monitor.Loggable._
import collection.Seqs._
import scalaz.{DList => _, _}
import Scalaz._
import org.apache.hadoop.mapreduce.Job

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
case class HadoopMode(sc: ScoobiConfiguration) extends Optimiser with MscrsDefinition with ShowNode {
  implicit lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

  /** execute a DList, storing the results in DataSinks */
  def execute(list: DList[_]) { execute(list.getComp) }
  /** execute a DObject, reading the result from a BridgeStore */
  def execute(o: DObject[_]): Any = execute(o.getComp)
  /** execute a computation graph */
  def execute(node: CompNode): Any = node |> prepare |> executeNode

  /**
   * Prepare the execution of the graph by optimising it
   */
  private
  lazy val prepare: CompNode => CompNode = attr("prepare") { case node =>
    optimise(node.debug("Raw nodes", prettyGraph)).debug("Optimised nodes", prettyGraph)
  }

  /**
   * execute a computation node
   */
  private
  lazy val executeNode: CompNode => Any = {
    def executeLayers(node: CompNode) {
      layers(node).debug("Executing layers", mkStrings).map(executeLayer)
    }
    // execute value nodes recursively, other nodes start a "layer" execution
    attr("executeNode") {
      case node @ Op1(in1, in2)    => node.execute(executeNode(in1), executeNode(in2)).debug("result for op "+node.id+": ")
      case node @ Return1(in)      => in
      case node @ Materialise1(in) => executeLayers(node); read(in.bridgeStore)
      case node                    => executeLayers(node)
    }
  }

  private lazy val executeLayer: Layer[T] => Any =
    attr("executeLayer") { case layer => Execution(layer).execute }

  /**
   * Execution of a "layer" of Mscrs
   */
  private case class Execution(layer: Layer[T]) {

    def execute: Seq[Any] = {
      if (layerBridgesAreFilled) debug("Skipping layer\n"+layer.id+" because all sinks have been filled")
      else                       runMscrs(mscrs(layer)).debug("Executing layer\n"+layer)

      layerBridges(layer).foreach(markBridgeAsFilled)
      layerBridges(layer).debug("Layer bridges: ").map(read).toSeq
    }

    /**
     * run mscrs concurrently if there are more than one.
     *
     * Only the execution part is done concurrently, not the configuration.
     * This is to make sure that there is not undesirable race condition during the setting up of variables
     */
    private def runMscrs(mscrs: Seq[Mscr]): Unit = {
      if (mscrs.size <= 1) mscrs.filterNot(isFilled).foreach(runMscr)
      else                 mscrs.filterNot(isFilled).toList.map(configureMscr).par.map(executeMscr.tupled)
    }

    /** @return true if the layer has bridges are they're all already filled by previous computations */
    private def layerBridgesAreFilled = layerBridges(layer).nonEmpty && layerBridges(layer).forall(hasBeenFilled)

    private def isFilled = (mscr: Mscr) =>
      (mscr.bridges.nonEmpty && mscr.bridges.forall(hasBeenFilled)).debug("Skipping Mscr\n"+mscr.id+" because all the sinks have been filled")

    /** run a Mscr */
    private def runMscr = (mscr: Mscr) => {
      implicit val mscrConfiguration = sc.duplicate
      executeMscr.tupled(configureMscr(mscr))
    }
    
    /** configure a Mscr */
    private def configureMscr = (mscr: Mscr) => {
      implicit val mscrConfiguration = sc.duplicate
      debug("Checking sources for mscr "+mscr.id+"\n"+mscr.sources.mkString("\n"))
      mscr.sources.foreach(_.inputCheck)

      debug("Checking the outputs for mscr "+mscr.id+"\n"+mscr.sinks.mkString("\n"))
      mscr.sinks.foreach(_.outputCheck)

      debug("Loading input nodes for mscr "+mscr.id+"\n"+mscr.inputNodes.mkString("\n"))
      mscr.inputNodes.foreach(load)

      (mscr, MapReduceJob(mscr).configure)
    }

    /** execute a Mscr */
    private def executeMscr = (mscr: Mscr, job: Job) => {
      implicit val mscrConfiguration = sc.duplicate
      job |> MapReduceJob(mscr).execute
    }
  }

  /** mark a bridge as filled so it doesn't have to be recomputed */
  private def markBridgeAsFilled = (b: Bridge) => filledBridge(b.bridgeStoreId)
  /** this attributes store the fact that a Bridge has received data */
  private lazy val filledBridge: CachedAttribute[String, String] = attr("filled bridge")(identity)
  /** @return true if a given Bridge has already received data */
  private def hasBeenFilled(b: Bridge)= filledBridge.hasBeenComputedAt(b.bridgeStoreId)
  /** @return the content of a Bridge as an Iterable */
  private lazy val read: Bridge => Any = attr("read") { case bs => bs.readAsIterable(sc) }

  /** make sure that all inputs environments are fully loaded */
  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case rt @ Return1(in)      => pushEnv(rt, in)
      case op @ Op1(in1, in2)    => pushEnv(op, op.execute(load(in1), load(in2)).debug("result for op "+op.id+": "))
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

