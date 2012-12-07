package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution._

import core._
import plan.comp._
import plan.mscr._
import monitor.Loggable._
import mapreducer.BridgeStore

/**
 * Execution of Scoobi applications using Hadoop.
 */
case class HadoopMode(implicit sc: ScoobiConfiguration) extends Optimiser with MscrsDefinition with ShowNode {
  implicit lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

  def execute(list: DList[_]): Unit = {
    execute(list.getComp)
  }

  def execute(o: DObject[_]): Any = {
    execute(o.getComp)
  }

  def execute(node: CompNode): Any = {
    executeNode((node, prepare(node)))
  }

  lazy val prepare: CompNode => CompNode = attr("prepare") { case node =>
    initAttributable(node)
    logger.debug("Raw nodes\n"+pretty(node))
    logger.debug("Raw graph\n"+showGraph(node))

    val optimised = initAttributable(optimise(node))
    logger.debug("Optimised nodes\n"+pretty(optimised))
    logger.debug("Optimised graph\n"+showGraph(optimised))
    optimised
  }

  lazy val executeNode: ((CompNode, CompNode)) => Any = {
    def executeLayers(original: CompNode, node: CompNode) {
      val graphLayers = (node -> layers)
      logger.debug("Executing layers\n"+graphLayers.mkString("\n"))
      graphLayers.map(executeLayer)
      markAsExecuted(original)
    }

    attr("executeNode") {
      case (original, node @ Op1(in1, in2)   ) => node.unsafeExecute(executeNode((original, in1)), executeNode((original, in2))).debug("result for op "+node.id+": ")
      case (original, node @ Return1(in)     ) => in
      case (original, node @ Materialize1(in)) => executeLayers(original, node); readNodeStore(node)
      case (original, node                   ) => executeLayers(original, node)
    }
  }

  private lazy val executeLayer: Layer[T] => Any = attr("executeLayer") { case layer =>
    Execution(layer).execute
  }

  case class Execution(layer: Layer[T]) {

    def execute: Seq[Any] = {
      logger.debug("Executing layer\n"+layer)

      val sources = layerSources(layer)
      logger.debug("Checking sources for layer "+layer.id+"\n"+sources.mkString("\n"))
      sources.foreach(_.inputCheck)

      val sourceNodes = layerSourceNodes(layer)
      logger.debug("Loading sources nodes for layer "+layer.id+"\n"+sourceNodes.mkString("\n"))
      sourceNodes.foreach(load)

      val sinks = layerSinks(layer)
      logger.debug("Checking the outputs for layer "+layer.id+"\n"+sinks.mkString("\n"))
      sinks.foreach(_.outputCheck)

      logger.debug("Executing the Mscrs for layer "+layer.id+"\n")
      mscrs(layer).zipWithIndex.foreach { case (mscr, step) =>
        val job = MapReduceJob.create(step, mscr)
        logger.debug("Executing Mscr\n"+mscr)
        job.run
      }
      sinks.debug("Layer sinks: ").map(readStore).toSeq
    }


  }

  /**
   * do not select nodes which have already been computed
   */
  override def selectNode(n: CompNode) =
    !hasBeenExecuted(n) && super.selectNode(n)

  private def markAsExecuted(node: CompNode) = (node +: descendents(node)).map(executed)

  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case mt @ Materialize1(in) => store(mt, readNodeStore(mt))
      case rt @ Return1(in)      => store(rt, in)
      case op @ Op1(in1, in2)    => store(op, op.unsafeExecute(load(in1), load(in2)).debug("result for op "+op.id+": "))
      case ld @ Load1(_)         => store(ld, ())
      case other                 => ()
    }
  }

  private def store(node: CompNode, result: Any)(implicit sc: ScoobiConfiguration) = {
    usesAsEnvironment(node).map { pd =>
      pd.unsafePushEnv(result)
    }
    result
  }

  private lazy val readNodeStore: CompNode => Any = attr("readNodeStore") {
    case mt: Materialize[_] => mt.in.bridgeStore.map(readBridgeStore).getOrElse(Seq())
    case other              => ()
  }

  private lazy val readStore: Sink => Any = attr("readStore") {
    case bs: BridgeStore[_] => readBridgeStore(bs)
    case other              => ()
  }

  private lazy val readBridgeStore: Bridge => Any = attr("readBridgeStore") {
    case bs => bs.readAsIterable
  }
}

