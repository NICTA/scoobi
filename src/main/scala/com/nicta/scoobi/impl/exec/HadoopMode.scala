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

  def execute(list: DList[_]) {
    executeNode(prepare(list.getComp))
  }

  def execute(o: DObject[_]) = {
    executeNode(prepare(o.getComp))
  }

  def prepare(node: CompNode) = {
    initAttributable(node)
    logger.debug("Raw nodes\n"+pretty(node))
    logger.debug("Raw graph\n"+showGraph(node))

    val optimised = initAttributable(optimise(node))
    resetMemo()

    logger.debug("Optimised nodes\n"+pretty(optimised))
    logger.debug("Optimised graph\n"+showGraph(optimised))
    optimised
  }

  lazy val executeNode: CompNode => Any = {
    def executeLayers(node: CompNode) {
      val graphLayers = (node -> layers)
      logger.debug("Executing layers\n"+graphLayers.mkString("\n"))
      graphLayers.map(layer => Execution(layer).execute).debug("Results: ")
    }

    attr("executeNode") {
      case node @ Op1(in1, in2)    => node.unsafeExecute(in1 -> executeNode, in2 -> executeNode).debug("result for op "+node.id+": ")
      case node @ Return1(in)      => in
      case node @ Materialize1(in) => executeLayers(node); readNodeStore(node).debug("result for materialize "+node.id+": ")
      case node                    => executeLayers(node)
    }
  }

  case class Execution(layer: Layer[T])(implicit sc: ScoobiConfiguration) {

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

  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case mt @ Materialize1(in) => store(mt, readNodeStore(mt).debug("result for materialize "+mt.id+": "))
      case rt @ Return1(in)      => store(rt, in)
      case op @ Op1(in1, in2)    => store(op, op.unsafeExecute(load(in1), load(in2)).debug("result for op "+op.id+": "))
      case ld @ Load1(_)         => store(ld, ())
      case other                 => ()
    }
  }

  private def store(node: CompNode, result: Any)(implicit sc: ScoobiConfiguration) = {
    (node -> usesAsEnvironment).headOption.map { pd =>
      pd.unsafePushEnv(result)
    }
    result
  }

  private lazy val readNodeStore: CompNode => Any = attr("readNodeStore") {
    case mt: Materialize[_] => if (mt.sinks.isEmpty) mt.in.bridgeStore.map(_.readAsIterable).getOrElse(Seq()) else Seq()
    case other              => ()
  }

  private lazy val readStore: Sink => Any = attr("readStore") {
    case bs: BridgeStore[_] => bs.readAsIterable
    case other              => ()
  }

}

