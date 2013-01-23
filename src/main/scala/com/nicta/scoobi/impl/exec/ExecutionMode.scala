package com.nicta.scoobi
package impl
package exec

import core._
import plan.comp._

trait ExecutionMode extends CompNodes {

  def checkSourceAndSinks(node: CompNode)(implicit sc: ScoobiConfiguration) {
    initAttributable(node)
    node match {
      case process: ProcessNode => process.sinks.foreach(_.outputCheck)
      case load: Load           => load.source.inputCheck
      case _                    => ()
    }
    children(node).foreach(n => checkSourceAndSinks(n))
  }
}
