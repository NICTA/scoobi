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
import org.apache.hadoop.mapreduce.{TaskAttemptID, Job}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import ScoobiConfigurationImpl._

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
    val toExecute = truncateAlreadyExecutedNodes(node.debug("Raw nodes", prettyGraph(showComputationGraph)))
    checkSourceAndSinks(sc)(toExecute.debug("Active nodes", prettyGraph(showComputationGraph)))
    toExecute
  }

  /** @return true (default value) if the computation graph must not be displayed */
  protected def showComputationGraph(implicit sc: ScoobiConfiguration) =
    sc.configuration.getBoolean("scoobi.debug.showComputationGraph", false)

  protected lazy val checkSourceAndSinks: CachedParamAttribute[ScoobiConfiguration, CompNode, Unit] = paramAttr("checkSourceAndSinks") { sc: ScoobiConfiguration => node: CompNode =>
    node.sinks.filterNot { case b: Bridge => hasBeenFilled(b); case _ => true }.foreach(_.outputCheck(sc))
    node match {
      case load: Load => load.source.inputCheck(sc)
      case _          => ()
    }
    children(node).foreach(n => checkSourceAndSinks(sc)(n))
  }

  def reset {
    resetMemo()
  }

  /**
   * @return the list of sinks to save for the node depending on the mode:
   *         In HadoopMode, bridges are already saved as part of the map reduce job
   *         In InMemoryMode all sinks need to be saved
   */
  protected def sinksToSave(node: CompNode): Seq[Sink] = node.sinks

  protected def saveSinks(values: Seq[Any], node: CompNode)(implicit sc: ScoobiConfiguration) {
    val sinks = sinksToSave(node)
    val valuesToSave = if (node.wf.isInstanceOf[WireFormat.TraversableWireFormat[_,_]]) values.head.asInstanceOf[Traversable[Any]] else values

    sinks.foreach(_.outputSetup(sc.configuration))
    sinks.foreach { sink =>
      val job = new Job(new Configuration(sc.configuration))

      val outputFormat = sink.outputFormat.newInstance

      sink.outputPath.foreach(FileOutputFormat.setOutputPath(job, _))
      job.setOutputFormatClass(sink.outputFormat)
      job.setOutputKeyClass(sink.outputKeyClass)
      job.setOutputValueClass(sink.outputValueClass)
      job.getConfiguration.set("mapreduce.output.basename", "ch0out0")  // Attempting to be consistent
      sink.configureCompression(job.getConfiguration)
      sink.outputConfigure(job)(sc)

      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rw = outputFormat.getRecordWriter(taskContext)
      val oc = outputFormat.getOutputCommitter(taskContext)

      oc.setupJob(job)
      oc.setupTask(taskContext)

      sink.write(valuesToSave, rw)(job.getConfiguration)

      rw.close(taskContext)
      oc.commitTask(taskContext)
      oc.commitJob(job)
    }
  }
}