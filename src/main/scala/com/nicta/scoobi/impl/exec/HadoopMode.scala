package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.kiama.attribution.Attribution._

import core._
import plan.comp.CompNodes._
import plan.comp.Optimiser
import plan.mscr._

/**
 * Execution of Scoobi applications using Hadoop.
 */
object HadoopMode extends Optimiser with MscrMaker with ExecutionPlan {
  lazy val logger = LogFactory.getLog("scoobi.HadoopMode")

  def execute(list: DList[_])(implicit sc: ScoobiConfiguration) {
    val optimised = optimise(list.getComp)
    val mscrs = makeMscrs(optimised)
    Execution(mscrs.toSeq).execute
  }

  def execute(o: DObject[_])(implicit sc: ScoobiConfiguration) = {
//    o.getComp -> prepare(sc)
//    o.getComp -> computeValue(sc)
    null
  }

  case class Execution(mscrs: Seq[Mscr])(implicit val sc: ScoobiConfiguration) {
    def execute = mscrs.foreach(_ -> compute)

    lazy val compute: Mscr => Unit =
      attr { (mscr: Mscr) =>
        (mscr -> inputMscrs).foreach(_ -> compute)
        MapReduceJob.create(1, mscr)
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
