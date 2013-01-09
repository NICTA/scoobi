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
    reinitAttributable(node)
    logger.debug("Raw nodes\n"+pretty(node))
    logger.debug("Raw graph\n"+showGraph(node))

    val optimised = reinitAttributable(optimise(node))
    reinitUses

    logger.debug("Optimised nodes\n"+pretty(optimised))
    logger.debug("Optimised graph\n"+showGraph(optimised))
    optimised
  }

  lazy val executeNode: ((CompNode, CompNode)) => Any = {
    def executeLayers(original: CompNode, node: CompNode) {
      val graphLayers = (node -> layers)
      logger.debug("Executing layers\n"+graphLayers.mkString("\n"))
      graphLayers.map(executeLayer)
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
      val bridgeStores = layerSinks(layer).collect { case bs: Bridge => bs }
      if (bridgeStores.forall(hasBeenFilled)) {
        logger.debug("Skipping layer\n"+layer.id+" because all sinks have been filled")
      } else {
        logger.debug("Executing layer\n"+layer)

        mscrs(layer).zipWithIndex.foreach { case (mscr, step) =>
          if (mscr.bridgeStores.forall(hasBeenFilled)) {
            logger.debug("Skipping Mscr\n"+mscr.id+" because all the sinks have been filled")
          } else {
            logger.debug("Executing Mscr\n"+mscr)

            logger.debug("Checking sources for mscr "+mscr.id+"\n"+mscr.sources.mkString("\n"))
            mscr.sources.foreach(_.inputCheck)

            logger.debug("Checking the outputs for mscr "+mscr.id+"\n"+mscr.sinks.mkString("\n"))
            mscr.sinks.foreach(_.outputCheck)

            logger.debug("Loading input nodes for mscr "+mscr.id+"\n"+mscr.inputNodes.mkString("\n"))
            mscr.inputNodes.foreach(load)

            val job = MapReduceJob.create(step, mscr)
            job.run
          }
        }
      }
      bridgeStores.map(bs => filledBridge(bs.bridgeStoreId))
      layerSinks(layer).debug("Layer sinks: ").map(readStore).toSeq
    }


  }

  private lazy val filledBridge: CachedAttribute[String, String] = attr("filled bridge")(identity)

  private def hasBeenFilled(b: Bridge): Boolean = {
    filledBridge.hasBeenComputedAt(b.bridgeStoreId)
  }

  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case rt @ Return1(in)      => pushEnv(rt, in)
      case op @ Op1(in1, in2)    => pushEnv(op, op.unsafeExecute(load(in1), load(in2)).debug("result for op "+op.id+": "))
      case ld @ Load1(_)         => pushEnv(ld, ())
      case other                 => pushEnv(other, readNodeStore(other))
    }
  }

  private def pushEnv(node: CompNode, result: Any)(implicit sc: ScoobiConfiguration) = {
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

