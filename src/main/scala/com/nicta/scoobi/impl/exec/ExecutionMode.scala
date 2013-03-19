/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package impl
package exec

import core._
import plan.comp._
import org.apache.commons.logging.Log
import monitor.Loggable._

trait ExecutionMode extends ShowNode with Optimiser {
  implicit def modeLogger: Log

  /** prepare the execution graph by:
    * - initialising the nodes
    * - truncating the graph if some nodes have already been executed
    * - checking the sources and sinks
    */
  protected def prepare(node: CompNode)(implicit sc: ScoobiConfiguration) = {
    reinitAttributable(node)
    reinitUses
    val toExecute = truncateAlreadyExecutedNodes(node.debug("Raw nodes", prettyGraph))
    checkSourceAndSinks(toExecute.debug("Active nodes", prettyGraph))
    toExecute
  }

  protected def checkSourceAndSinks(node: CompNode)(implicit sc: ScoobiConfiguration) {
    initAttributable(node)
    node match {
      case process: ProcessNode => process.sinks.filterNot { case b: Bridge => hasBeenFilled(b); case _ => true }.foreach(_.outputCheck)
      case load: Load           => load.source.inputCheck
      case _                    => ()
    }
    children(node).foreach(n => checkSourceAndSinks(n))
  }

  def reset {
    resetMemo()
  }
}
