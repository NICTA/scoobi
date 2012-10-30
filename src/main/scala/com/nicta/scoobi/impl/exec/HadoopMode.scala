package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution.Attribution._

import core._
import plan.comp.CompNodes._
import plan.comp.Optimiser
import plan.mscr._
import mapreducer.BridgeStore

/**
 * Execution of Scoobi applications using Hadoop.
 */
object HadoopMode extends Optimiser with MscrMaker with ExecutionPlan {
  lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

  def execute(list: DList[_])(implicit sc: ScoobiConfiguration) {
    executeNode(list.getComp)
  }

  def execute(o: DObject[_])(implicit sc: ScoobiConfiguration) = {
    executeNode(o.getComp)
    o.getComp.sinks.flatMap(_.asInstanceOf[BridgeStore[_]].readAsIterable).head
  }

  private def executeNode(node: CompNode)(implicit sc: ScoobiConfiguration) = {
    val optimised = optimise(node)
    val mscrs = makeMscrs(optimised)
    Execution(mscrs.toSeq).execute
  }

  case class Execution(mscrs: Seq[Mscr])(implicit val sc: ScoobiConfiguration) {
    def execute = mscrs.foreach(_ -> compute)

    private var step = 0

    lazy val compute: Mscr => Unit =
      attr { (mscr: Mscr) =>
        // compute first the dependent mscrs
        (mscr -> inputMscrs).foreach(_ -> compute)
        step += 1
        val job = MapReduceJob.create(step, mscr)
        job.run
        ()
      }

    lazy val inputMscrs: Mscr => Seq[Mscr] =
      attr { mscr: Mscr =>
        mscr.inputChannels.toSeq.flatMap(_ -> inputChannelMscrs)
      }

    lazy val inputChannelMscrs: InputChannel => Seq[Mscr] =
      attr {
        case MapperInputChannel(pdos) => pdos.toSeq.flatMap(node => mscrs.filter(_.outputContains(node)))
        case IdInputChannel(in, _)    => mscrs.filter(_.outputContains(in))
        case StraightInputChannel(in) => mscrs.filter(_.outputContains(in))
      }
  }
}
