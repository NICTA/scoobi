package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution.Attribution._

import core._
import plan.comp._
import ShowNode._
import plan.mscr._
import mapreducer.BridgeStore
import CompNodes._
import ScoobiConfigurationImpl._

/**
 * Execution of Scoobi applications using Hadoop.
 */
case class HadoopMode(implicit sc: ScoobiConfiguration) extends Optimiser with MscrMaker with ExecutionPlan {
  lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

  def execute(list: DList[_]) {
    executeNode(list.getComp)
  }

  def execute(o: DObject[_]) = {
    executeNode(o.getComp)
  }

  lazy val executeNode: CompNode => Any = attr { node => {

    initAttributable(node)
    logger.debug("Raw nodes\n"+pretty(node))
    logger.debug("Raw graph\n"+showGraph(node))

    node match {
      case mt @ Materialize1(in) => store(mt, mt.in -> executeNode)
      case op @ Op1(in1, in2)    => store(op, op.unsafeExecute(in1 -> executeNode, in2 -> executeNode))
      case rt @ Return1(in)      => store(rt, in)
      case ld @ Load1(_)         => store(ld, ())
      case other                 => executeMscr(other)
    }
  }}

  private def store(node: CompNode, execute: Any)(implicit sc: ScoobiConfiguration) = {
    val result = (node match {
      case Materialize1(in) => in.sinks.collect { case bs: BridgeStore[_] => bs.readAsIterable }.headOption
      case _                => None
    }).getOrElse(execute)

    (node -> usesAsEnvironment).headOption.map(_.unsafePushEnv(result))
    result
  }

  private def executeMscr(node: CompNode)(implicit sc: ScoobiConfiguration) {
    val optimised = initAttributable(optimise(node))

    logger.debug("Optimised nodes\n"+pretty(optimised))
    logger.debug("Optimised graph\n"+showGraph(optimised))
    val mscrs = makeMscrs(optimised)

    logger.debug("Executing Mscrs\n"+mscrsGraph(optimised))
    Execution(mscrs.toSeq).execute
  }

  case class Execution(mscrs: Seq[Mscr]) {
    def execute = mscrs.foreach(_ -> compute)

    private var step = 0

    lazy val compute: Mscr => Unit = attr { (mscr: Mscr) =>
      // compute first the dependent mscrs
      mscr.inputs.foreach(_ -> executeNode)

      step += 1
      val job = MapReduceJob.create(step, mscr)
      job.run
      ()
    }

  }
}

