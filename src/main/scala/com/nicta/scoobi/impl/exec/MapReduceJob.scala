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

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.mapred.{JobConf, TaskCompletionEvent}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{WritableComparable, WritableComparator, RawComparator}
import scalaz.syntax.all._

import core._
import plan._
import com.nicta.scoobi.impl.plan.mscr.{GbkOutputChannel, OutputChannels, InputChannels, Mscr}
import rtt._
import impl.util._
import reflect.Classes._
import io._
import mapreducer._
import ScoobiConfigurationImpl._
import ScoobiConfiguration._
import MapReduceJob.configureJar
import control.Exceptions._
import monitor.Loggable
import Loggable._
import mapreducer.BridgeStore
import Configurations._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ReflectionUtils

/**
 * A class that defines a single Hadoop MapReduce job and configures Hadoop based on the Mscr to execute
 */
case class MapReduceJob(mscr: Mscr, layerId: Int)(implicit val configuration: ScoobiConfiguration) {

  implicit protected val fileSystems: FileSystems = FileSystems
  private implicit lazy val logger = LogFactory.getLog("scoobi.MapReduceJob")

  implicit lazy val job = {
    configuration.jobStepIs(mscr.id)
    new Job(configuration, configuration.jobId+"-"+configuration.jobStep)
  }
  
  /** Take this MapReduce job and run it on Hadoop. */
  def run = {
    configure
    execute
    report
  }

  /** execute the Hadoop job and collect results */
  def execute = {
    ("STARTED executing MSCR "+mscr.id+" on layer "+layerId).debug
    executeJob
    ("FINISHED executing MSCR "+mscr.id+" on layer "+layerId).debug
    collectOutputs
  }

  def report = {
    // if job failed, throw an exception
    // an IllegalStateException can be thrown when asking for job.isSuccessful if the job has started executing but the
    // RUNNING state has not been set
    val successful = tryOrElse(job.isSuccessful)(false)
    if(!successful) {
      throw new JobExecException("MapReduce job '" + job.getJobID + "' failed!" + tryOrElse(" Please see " + job.getTrackingURL + " for more info.")(""))
    }
    this
  }

  /** configure the Hadoop job */
  def configure = {
    val jar = new JarBuilder
    job.getConfiguration.set("mapred.jar", configuration.temporaryJarFile.getAbsolutePath)

    configureKeysAndValues(jar, job.getConfiguration)
    configureMappers(jar)
    configureCombiners(jar)
    configureReducers(jar)
    configureJar(jar)
    jar.close(configuration)

    FileOutputFormat.setOutputPath(job, configuration.temporaryOutputDirectory(job))

    this
  }


  /** Sort-and-shuffle:
   *   - (K2, V2) are (TaggedKey, TaggedValue), the wrappers for all K-V types
   *   - Partitioner is generated and of type TaggedPartitioner
   *   - GroupingComparator is generated and of type TaggedGroupingComparator
   *   - SortComparator is handled by TaggedKey which is WritableComparable */
  private def configureKeysAndValues(jar: JarBuilder, jobConfiguration: Configuration) {
    val id = UniqueId.get

    val tkRtClass = TaggedKey("TK" + id, mscr.keyTypes.types, configuration.scoobiClassLoader, jobConfiguration)
    jar.addRuntimeClass(tkRtClass)
    job.setMapOutputKeyClass(tkRtClass.clazz)

    val tvRtClass = TaggedValue("TV" + id, mscr.valueTypes.types, configuration.scoobiClassLoader, jobConfiguration)
    jar.addRuntimeClass(tvRtClass)
    job.setMapOutputValueClass(tvRtClass.clazz)

    val tpRtClass = TaggedPartitioner("TP" + id, mscr.keyTypes.types, configuration.scoobiClassLoader, jobConfiguration)
    jar.addRuntimeClass(tpRtClass)
    job.setPartitionerClass(tpRtClass.clazz.asInstanceOf[Class[_ <: Partitioner[_,_]]])

    val tgRtClass = TaggedGroupingComparator("TG" + id, mscr.keyTypes.types, configuration.scoobiClassLoader, jobConfiguration)
    jar.addRuntimeClass(tgRtClass)
    job.setGroupingComparatorClass(tgRtClass.clazz.asInstanceOf[Class[_ <: RawComparator[_]]])

    // A special kind of comparator needs to be added for TaggedKeys
    job.setSortComparatorClass(classOf[ConfiguredWritableComparator])
  }



  /** Mappers:
   *     - use ChannelInputs to specify multiple mappers through job
   *     - generate runtime class (ScoobiWritable) for each input value type and add to JAR (any
   *       mapper for a given input channel can be used as they all have the same input type */
  private def configureMappers(jar: JarBuilder) {
    ChannelsInputFormat.configureSources(job, jar, mscr.sources)

    DistCache.pushObject(job.getConfiguration, InputChannels(mscr.inputChannels), s"scoobi.mappers-${configuration.jobStep}")
    job.setMapperClass(classOf[MscrMapper].asInstanceOf[Class[_ <: Mapper[_,_,_,_]]])
  }

  /** Combiners:
   *   - only need to make use of Hadoop's combiner facility if actual combiner
   *   functions have been added
   *   - use distributed cache to push all combine code out */
  private def configureCombiners(jar: JarBuilder) {
    if (!mscr.combiners.isEmpty) {
      DistCache.pushObject(job.getConfiguration, mscr.combinersByTag, s"scoobi.combiners-${configuration.jobStep}")
      job.setCombinerClass(classOf[MscrCombiner].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])
    }
  }

  /** Reducers:
   *     - generate runtime class (ScoobiWritable) for each output values being written to
   *       a BridgeStore and add to JAR
   *     - add a named output for each output channel */
  private def configureReducers(jar: JarBuilder) {
    mscr.sinks collect { case bs : BridgeStore[_]  =>
      val rtClass = bs.rtClass(ScoobiConfiguration(job.getConfiguration)).debug(c => "adding the BridgeStore class "+c.clazz.getName+" from Sink "+bs.id+" to the configuration")
      jar.addRuntimeClass(rtClass)
    }

    mscr.outputChannels.foreach { out =>
      out.sinks.foreach(sink => ChannelOutputFormat.addOutputChannel(job, out.tag, sink))
    }

    DistCache.pushObject(job.getConfiguration, OutputChannels(mscr.outputChannels), s"scoobi.reducers-${configuration.jobStep}")
    job.setReducerClass(classOf[MscrReducer].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])

    if (mscr.gbkOutputChannels.isEmpty) {
      job.setNumReduceTasks(0)
      logger.info("There are no reducers for this job")
    }
    else {
      /**
       * Calculate the number of reducers to use with a simple heuristic:
       *
       * Base the amount of parallelism required in the reduce phase on the size of the data output. Further,
       * estimate the size of output data to be the size of the input data to the MapReduce job. Then, set
       * the number of reduce tasks to the number of 1GB data chunks in the estimated output. */
      val inputBytes: Long = mscr.sources.map(_.inputSize).sum
      val inputGigabytes: Int = (inputBytes / (configuration.getBytesPerReducer)).toInt + 1
      val numReducers: Int = inputGigabytes.max(configuration.getMinReducers).min(configuration.getMaxReducers)
      job.setNumReduceTasks(numReducers)

      /* Log stats on this MR job. */
      logger.info("Total input size: " +  Helper.sizeString(inputBytes))
      logger.info("Number of reducers: " + numReducers)
    }

  }

  private def executeJob = {

    val taskDetailsLogger = new TaskDetailsLogger(job)

    try {
      /* Run job */
      job.submit()

      val map    = new Progress(job.mapProgress())
      val reduce = new Progress(job.reduceProgress())

      logger.info("MapReduce job '" + job.getJobID + "' submitted. Please see " + job.getTrackingURL + " for more info.")

      while (!job.isComplete) {
        Thread.sleep(configuration.getInt("scoobi.progress.time", 5000))
        if (map.hasProgressed || reduce.hasProgressed)
          logger.info("Map " + map.getProgress.formatted("%3d") + "%    " +
            "Reduce " + reduce.getProgress.formatted("%3d") + "%")

        // Log task details
        taskDetailsLogger.logTaskCompletionDetails()
      }
    } finally {
      configuration.temporaryJarFile.delete
    }
    // Log any left over task details
    taskDetailsLogger.logTaskCompletionDetails()
    this
  }

  private[scoobi] def collectOutputs = {
    /* Move named file-based sinks to their correct output paths. */
    mscr.outputChannels.foreach(_.collectOutputs(fileSystems.listPaths(configuration.temporaryOutputDirectory(job))))
    configuration.deleteTemporaryOutputDirectory(job)
    configuration.updateCounters(job.getCounters)
    this
  }
}

private[scoobi]
object MapReduceJob {
  private implicit lazy val logger = LogFactory.getLog("scoobi.MapReduceJob")

  /**
   * Make temporary JAR file for this job. At a minimum need the Scala runtime
   * JAR, the Scoobi JAR, and the user's application code JAR(s)
   */
  def configureJar(jar: JarBuilder)(implicit configuration: ScoobiConfiguration) {
    // if the dependent jars have not been already uploaded, make sure that the Scoobi jar
    // i.e. the jar containing this.getClass, is included in the job jar.
    // add also the client jar containing the main method executing the job
    if (!configuration.uploadedLibJars) {
      jar.addContainingJar(getClass)
      jar.addContainingJar(mainClass)
    }
    configuration.userJars.foreach { jar.addJar(_) }
    configuration.userDirs.foreach { jar.addClassDirectory(_) }
    configuration.userClasses.map { case (name, bytecode) => jar.addClassFromBytecode(name, bytecode) }
  }

}

/* Helper class to track progress of Map or Reduce tasks and whether or not
 * progress has advanced. */
class Progress(updateFn: => Float) {
  private var progressed = true
  private var progress = (updateFn * 100).toInt

  def hasProgressed = {
    val p = (updateFn * 100).toInt
    if (p > progress) { progressed = true; progress = p } else { progressed = false }
    progressed
  }

  def getProgress: Int = {
    hasProgressed
    progress
  }
}

class TaskDetailsLogger(job: Job) {

  import TaskCompletionEvent.Status._

  private lazy val logger = LogFactory.getLog("scoobi.Step")

  private var startIdx = 0

  /** Paginate through the TaskCompletionEvent's, logging details about completed tasks */
  def logTaskCompletionDetails() {
    Iterator.continually(job.getTaskCompletionEvents(startIdx)).takeWhile(!_.isEmpty).foreach { taskCompEvents =>
      taskCompEvents foreach { taskCompEvent =>
        val taskAttemptId = taskCompEvent.getTaskAttemptId
        val logUrl = createTaskLogUrl(taskCompEvent.getTaskTrackerHttp, taskAttemptId.toString)
        val taskAttempt = "Task attempt '"+taskAttemptId+"'"
        val moreInfo = " Please see "+logUrl+" for task attempt logs"
        taskCompEvent.getTaskStatus match {
          case OBSOLETE  => logger.debug(taskAttempt + " was made obsolete." + moreInfo)
          case FAILED    => logger.info(taskAttempt + " failed! " + "Trying again." + moreInfo)
          case KILLED    => logger.debug(taskAttempt + " was killed!" + moreInfo)
          case TIPFAILED => logger.error("Task '" + taskAttemptId.getTaskID + "' failed!" + moreInfo)
          case _ =>
        }
      }
      startIdx += taskCompEvents.length
    }
  }

  private def createTaskLogUrl(trackerUrl: String, taskAttemptId: String): String = {
    trackerUrl + "/tasklog?attemptid=" + taskAttemptId + "&all=true"
  }
}

class JobExecException(msg: String) extends RuntimeException(msg)

/**
 * This Comparator delegates comparisons to the WritableComparator for the MapOutputKeyClass
 * but first it instantiate keys using the configuration (instead of null in the original version)
 * so that the TaggedKey can be set up with the proper metadata
 */
class ConfiguredWritableComparator extends RawComparator[WritableComparable[_]] with Configured {
  override def setConf(conf: Configuration) = {
    super.setConf(conf)
    WritableComparator.define(keyClass, new WritableComparator(keyClass, true) {
      override def newKey = ReflectionUtils.newInstance(keyClass, configuration)
    })
  }

  lazy val keyClass = new JobConf(configuration).getMapOutputKeyClass.asInstanceOf[Class[_ <: WritableComparable[_]]]
  def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int) = WritableComparator.get(keyClass).compare(b1, s1, l1, b2, s2, l2)

  def compare(a: WritableComparable[_], b: WritableComparable[_]) = WritableComparator.get(keyClass).compare(a, b)
}

