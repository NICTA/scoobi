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
import org.apache.hadoop.mapreduce.{TaskType, TaskAttemptID, Job}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import ScoobiConfigurationImpl._
import util.Compatibility
import com.nicta.scoobi.impl.rtt.ScoobiMetadata
import org.apache.hadoop.util.ReflectionUtils
import com.nicta.scoobi.impl.io.{FileSystems, Files}
import org.apache.hadoop.fs.Path

trait ExecutionMode extends ShowNode with Optimiser {
  implicit protected def modeLogger: Log

  /** prepare the execution graph by:
    * - initialising the nodes
    * - truncating the graph if some nodes have already been executed
    * - checking the sources and sinks
    */
  protected def prepare(node: CompNode)(implicit sc: ScoobiConfiguration) = {
    reinit(node)
    val toExecute = truncateAlreadyExecutedNodes(node.debug("Raw nodes", prettyGraph(showComputationGraph)))
    checkSourceAndSinks(toExecute.debug("Active nodes", prettyGraph(showComputationGraph)))
    toExecute
  }

  /** @return false (default value) if the computation graph must not be displayed */
  protected def showComputationGraph(implicit sc: ScoobiConfiguration) =
    sc.configuration.getBoolean("scoobi.debug.showComputationGraph", false)

  /** @return false (default value) if the execution must only be shown but not executed */
  protected def showPlanOnly(implicit sc: ScoobiConfiguration) =
    sc.configuration.getBoolean("scoobi.debug.showPlanOnly", false)

  protected def checkSourceAndSinks(node: CompNode)(implicit sc: ScoobiConfiguration): Unit = {
    def checkNode(n: CompNode) = {
      n.sinks.filterNot(hasBeenFilled).foreach(_.outputCheck(sc))
      n match {
        case load: Load => load.source.inputCheck(sc)
        case _          => ()
      }
    }
    checkNode(node)
    descendents(node).distinct.foreach(checkNode)
  }

  /**
   * @return the list of sinks to save for the node depending on the mode:
   *         In HadoopMode, bridges are already saved as part of the map reduce job
   *         In InMemoryMode all sinks need to be saved
   */
  protected def sinksToSave(node: CompNode): Seq[Sink]

  protected def saveSinks(values: Seq[Any], node: CompNode)(implicit sc: ScoobiConfiguration) {
    val sinks = sinksToSave(node)
    val valuesToSave = if (WireFormat.isTraversable(node.wf)) values.head.asInstanceOf[Traversable[Any]] else values

    // if the value type is a ScoobiWritable we need to store its wireformat as metadata in the distributed cache
    // even if the execution is in memory
    val elementWireformat = node.wf match {
      case twf: WireFormat.TraversableWireFormat[_,_] => twf.elementWireFormat
      case _ => node.wf
    }

    sinks.foreach(_.outputSetup(sc))
    sinks.foreach { sink =>
      val job = Compatibility.newJob(new Configuration(sc.configuration))

      val outputFormat = ReflectionUtils.newInstance(sink.outputFormat, job.getConfiguration)

      sink.outputPath.foreach(FileOutputFormat.setOutputPath(job, _))
      job.setOutputFormatClass(sink.outputFormat)

      job.setOutputKeyClass(sink.outputKeyClass)

      job.setOutputValueClass(sink.outputValueClass)
      ScoobiMetadata.saveMetadata("scoobi.metadata."+sink.outputValueClass.getName, elementWireformat, job.getConfiguration)

      job.getConfiguration.set("mapreduce.output.basename", s"ch${node.id}out${sink.id}")
      job.getConfiguration.set("mapred.work.output.dir", sink.outputPath.getOrElse(sink.id).toString)
      job.getConfiguration.set("avro.mo.config.namedOutput", s"ch${node.id}out${sink.id}")

      sink.configureCompression(job.getConfiguration)
      sink.outputConfigure(job)(sc)

      val taskContext = Compatibility.newTaskAttemptContext(job.getConfiguration, TaskAttemptID.forName("attempt_201301010000_0001_m_000001_0"))
      val rw = outputFormat.getRecordWriter(taskContext)
      val oc = outputFormat.getOutputCommitter(taskContext)

      oc.setupJob(job)
      oc.setupTask(taskContext)

      sink.write(valuesToSave, rw)(job.getConfiguration)

      rw.close(taskContext)
      oc.commitTask(taskContext)
      oc.commitJob(job)

      implicit val fs = sc.fileSystem

      sink.outputPath.foreach { outputDir =>
        val outputs = FileSystems.listPaths(outputDir)

        outputs foreach { outputFilePath =>

          val fromOutputDir = outputFilePath.toUri.getPath.replace(Files.dirPath(outputDir.toUri.getPath), "")
          val withoutAttempt = fromOutputDir.split("/").filterNot(n => Seq("_attempt", "_temporary").exists(n.startsWith)).mkString("/")

          if (withoutAttempt != fromOutputDir) {
            val newPath = new Path(withoutAttempt)
            Files.moveTo(outputDir)(sc.configuration).apply(outputFilePath, newPath)
          }
        }
      }
    }
    sinks.foreach(_.outputTeardown(sc))
  }
}