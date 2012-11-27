package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution._
import Attributable._

import core._
import plan.comp._
import plan.mscr._
import ScoobiConfigurationImpl._

/**
 * Execution of Scoobi applications using Hadoop.
 */
case class HadoopMode(implicit sc: ScoobiConfiguration) extends Optimiser with MscrsDefinition with ShowNode {
  lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

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

  lazy val executeNode: CompNode => Any = attr { node =>
    val graphLayers = (node -> layers)
    graphLayers.flatMap(layerResults).collect {
      case rt @ Return1(in)      => store(rt, in)
      case ld @ Load1(_)         => store(ld, ())
    }
    val result = graphLayers.flatMap { layer =>
      Execution(layer).execute
    }.headOption.getOrElse(())
    result
  }

  case class Execution(layer: Layer[T])(implicit sc: ScoobiConfiguration) {
    private var step = 0
    def execute: Seq[Any] = {
      layerResults(layer).collect {
        case mt @ Materialize1(in) => store(mt, readStore(in))
        case op @ Op1(in1, in2)    => store(op, op.unsafeExecute(readStore(in1), readStore(in2)))
      }

      mscrs(layer).foreach { mscr =>
        step += 1
        val job = MapReduceJob.create(step, mscr)
        logger.debug("Executing Mscr\n"+mscr)
        job.run
      }

      layerResults(layer).collect {
        case mt @ Materialize1(in) => store(mt, readStore(mt))
      }
    }


  }

  private def store(node: CompNode, result: Any)(implicit sc: ScoobiConfiguration) = {
    (node -> usesAsEnvironment).headOption.map(pd => pd.unsafePushEnv(result))
    result
  }
  private def readStore(node: CompNode): Any = {
    node match {
      case Return1(in)        => in
      case Materialize1(in)   => in.bridgeStore.map(_.readAsIterable).getOrElse(())
      case op @ Op1(in1, in2) => op.unsafeExecute(readStore(in1), readStore(in2))
      case other              => other.bridgeStore.map(_.readAsIterable).getOrElse(())
    }
  }

}

