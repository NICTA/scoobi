package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution._

import core._
import plan.comp._
import plan.mscr._
import monitor.Loggable._

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

    logger.debug("Optimised nodes\n"+pretty(optimised))
    logger.debug("Optimised graph\n"+showGraph(optimised))
    optimised
  }

  lazy val executeNode: CompNode => Any = {
    def executeLayers(node: CompNode) = {
      val graphLayers = (node -> layers)
      logger.debug("Executing layers\n"+graphLayers.mkString("\n"))
      graphLayers.map { layer => Execution(layer).execute }.debug("Results: ")
      ()
    }
    attr {
      case node @ Op1(in1, in2)    => node.unsafeExecute(in1 -> executeNode, in2 -> executeNode).debug("result for op "+node.id+": ")
      case node @ Return1(in)      => in
      case node @ Materialize1(in) => executeLayers(node); readStore(node).debug("result for materialize "+node.id+": ")
      case node                    => executeLayers(node)
    }
  }

  case class Execution(layer: Layer[T])(implicit sc: ScoobiConfiguration) {
    private var step = 0
    def execute: Seq[Any] = {
      logger.debug("Executing layer\n"+layer)

      val sources = layerSources(layer)
      logger.debug("Loading sources for layer "+layer.id+"\n"+sources.mkString("\n"))
      sources.foreach(load)

      logger.debug("Executing the Mscrs for layer "+layer.id+"\n")
      mscrs(layer).foreach { mscr =>
        step += 1
        val job = MapReduceJob.create(step, mscr)
        logger.debug("Executing Mscr\n"+mscr)
        job.run
      }

      layerSinks(layer).debug("Layer sinks: ").map(readStore).toSeq
    }


  }

  private def load(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    node match {
      case mt @ Materialize1(in) => store(mt, readStore(mt).debug("result for materialize "+mt.id+": "))
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

  private lazy val readStore: CompNode => Any = attr {
    case mt @ Materialize1(in) => in.bridgeStore.map(_.readAsIterable).getOrElse(())
    case other                 => ()
  }

}

