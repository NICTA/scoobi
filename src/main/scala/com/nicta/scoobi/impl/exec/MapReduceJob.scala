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
import org.apache.hadoop.mapred.TaskCompletionEvent
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.RawComparator
import scalaz.syntax.all._

import core._
import plan._
import mscr.{MscrJob, Mscr}
import rtt._
import util._
import reflect.Classes._
import io._
import mapreducer._
import ScoobiConfigurationImpl._
import ChannelOutputFormat._
import monitor.Loggable._
import org.apache.hadoop.fs.Path

/** A class that defines a single Hadoop MapReduce job. */
case class MapReduceJob(stepId: Int,
                        mappers:   MscrMappers   = MscrMappers(),
                        reducers:  MscrReducers  = MscrReducers(),
                        combiners: MscrCombiners = MscrCombiners(),
                        keyTypes: KeyTypes       = KeyTypes(),
                        valueTypes: ValueTypes   = ValueTypes(),
                        mscr: Mscr               = Mscr()) extends MscrJob {

  type T = MapReduceJob

  implicit protected val fileSystems: FileSystems = FileSystems

  private implicit lazy val logger = LogFactory.getLog("scoobi.Step")

  /** Add an input mapping function to this MapReduce job. */
  def addTaggedMapper(inputs: Seq[Source], env: Option[Env[_]], mapper: TaggedMapper): MapReduceJob =
    inputs.foldLeft(this) { (job, input) => job.addTaggedMapper(input, env, mapper) }

  def addTaggedMapper(input: Source, env: Option[Env[_]], mapper: TaggedMapper): MapReduceJob =
    addMapper(input, env, mapper).addTaggedTypes(mapper)

  def addMapper(input: Source, env: Option[Env[_]], mapper: TaggedMapper): MapReduceJob =
    copy(mappers = mappers.add(input, env.getOrElse(Env.empty), mapper))

 def addTaggedTypes(mapper: TaggedMapper) =
   mapper.tags.foldLeft(this) { (job, tag) =>
     job.addKeyType  (tag,  mapper.wfk, mapper.gpk).
         addValueType(tag,  mapper.wfv)
   }

 def addKeyType(tag: Int, wf: WireFormat[_], gp: Grouping[_]) =
   copy(keyTypes = keyTypes.add(tag, wf, gp))

 def addValueType(tag: Int, wf: WireFormat[_]) =
   copy(valueTypes = valueTypes.add(tag, wf))

  /** Add a combiner function to this MapReduce job. */
  def addTaggedCombiner[V](combiner: TaggedCombiner[_]) =
    copy(combiners = combiners.add(combiner))

  /** Add an output reducing function to this MapReduce job. */
  def addTaggedReducer(outputs: Seq[Sink], env: Option[Env[_]], reducer: TaggedReducer) =
    copy(reducers = reducers.add(outputs, env.getOrElse(Env.empty), reducer))

  /** Take this MapReduce job and run it on Hadoop. */
  def run(implicit configuration: ScoobiConfiguration) {

    val job =
      new Job(configuration, configuration.jobStep(stepId)) |>
      configureJob |>
      executeJob   |>
      collectOutputs

    // if job failed, throw an exception
    if(!job.isSuccessful) {
      throw new JobExecException("MapReduce job '" + job.getJobID + "' failed! Please see " + job.getTrackingURL + " for more info.")
    }
  }
  
  def configureChannels(implicit configuration: ScoobiConfiguration) = mscr.channels.foldRight(this)(_.configure(_))

  def configureJob(implicit configuration: ScoobiConfiguration) = (job: Job) => {
    FileOutputFormat.setOutputPath(job, configuration.temporaryOutputDirectory)

    val jar = new JarBuilder
    job.getConfiguration.set("mapred.jar", configuration.temporaryJarFile.getAbsolutePath)
    configureKeysAndValues(jar, job)
    configureMappers(jar, job)
    configureCombiners(jar, job)
    configureReducers(jar, job)
    configureJar(jar)
    jar.close(configuration)

    job
  }

  /** Make temporary JAR file for this job. At a minimum need the Scala runtime
   * JAR, the Scoobi JAR, and the user's application code JAR(s). */
  private[scoobi] def configureJar(jar: JarBuilder)(implicit configuration: ScoobiConfiguration) {
    // if the dependent jars have not been already uploaded, make sure that the Scoobi jar
    // i.e. the jar containing this.getClass, is included in the job jar.
    // add also the client jar containing the main method executing the job
    if (!configuration.uploadedLibJars) {
      jar.addContainingJar(getClass)
      jar.addContainingJar(mainClass)
    }
    configuration.userJars.foreach { jar.addJar(_) }
    configuration.userDirs.foreach { jar.addClassDirectory(_) }
  }

  /** Sort-and-shuffle:
   *   - (K2, V2) are (TaggedKey, TaggedValue), the wrappers for all K-V types
   *   - Partitioner is generated and of type TaggedPartitioner
   *   - GroupingComparator is generated and of type TaggedGroupingComparator
   *   - SortComparator is handled by TaggedKey which is WritableComparable */
  private def configureKeysAndValues(jar: JarBuilder, job: Job)(implicit configuration: ScoobiConfiguration) {
    val id = UniqueId.get

    val tkRtClass = TaggedKey("TK" + id, keyTypes.types)
    jar.addRuntimeClass(tkRtClass)
    job.setMapOutputKeyClass(tkRtClass.clazz)

    val tvRtClass = TaggedValue("TV" + id, valueTypes.types)
    jar.addRuntimeClass(tvRtClass)
    job.setMapOutputValueClass(tvRtClass.clazz)

    val tpRtClass = TaggedPartitioner("TP" + id, keyTypes.types)
    jar.addRuntimeClass(tpRtClass)
    job.setPartitionerClass(tpRtClass.clazz.asInstanceOf[Class[_ <: Partitioner[_,_]]])

    val tgRtClass = TaggedGroupingComparator("TG" + id, keyTypes.types)
    jar.addRuntimeClass(tgRtClass)
    job.setGroupingComparatorClass(tgRtClass.clazz.asInstanceOf[Class[_ <: RawComparator[_]]])
  }

  /** Mappers:
   *     - use ChannelInputs to specify multiple mappers through job
   *     - generate runtime class (ScoobiWritable) for each input value type and add to JAR (any
   *       mapper for a given input channel can be used as they all have the same input type */
  private def configureMappers(jar: JarBuilder, job: Job)(implicit configuration: ScoobiConfiguration) {
    ChannelsInputFormat.configureSources(job, jar, mappers.sources)

    DistCache.pushObject(job.getConfiguration, mappers.inputs, "scoobi.mappers")
    job.setMapperClass(classOf[MscrMapper[_,_,_,_,_,_]].asInstanceOf[Class[_ <: Mapper[_,_,_,_]]])
  }

  /** Combiners:
   *   - only need to make use of Hadoop's combiner facility if actual combiner
   *   functions have been added
   *   - use distributed cache to push all combine code out */
  private def configureCombiners(jar: JarBuilder, job: Job)(implicit configuration: ScoobiConfiguration) {
    if (!combiners.isEmpty) {
      DistCache.pushObject(job.getConfiguration, combiners.combinersByTag, "scoobi.combiners")
      job.setCombinerClass(classOf[MscrCombiner[_]].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])
    }
  }

  /** Reducers:
   *     - generate runtime class (ScoobiWritable) for each output values being written to
   *       a BridgeStore and add to JAR
   *     - add a named output for each output channel */
  private def configureReducers(jar: JarBuilder, job: Job)(implicit configuration: ScoobiConfiguration) {
    reducers.configureBridgeStore(jar)
    reducers.configureOutputChannel(job)

    DistCache.pushObject(job.getConfiguration, reducers.outputs, "scoobi.reducers")
    job.setReducerClass(classOf[MscrReducer[_,_,_,_,_,_]].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])


    /* Calculate the number of reducers to use with a simple heuristic:
     *
     * Base the amount of parallelism required in the reduce phase on the size of the data output. Further,
     * estimate the size of output data to be the size of the input data to the MapReduce job. Then, set
     * the number of reduce tasks to the number of 1GB data chunks in the estimated output. */
    val inputBytes: Long = mappers.sources.map(_.inputSize).sum
    val inputGigabytes: Int = (inputBytes / (configuration.getBytesPerReducer)).toInt + 1
    val numReducers: Int = inputGigabytes.max(configuration.getMinReducers).min(configuration.getMaxReducers)
    job.setNumReduceTasks(numReducers)

    /* Log stats on this MR job. */
    logger.info("Total input size: " +  Helper.sizeString(inputBytes))
    logger.info("Number of reducers: " + numReducers)
  }

  private def executeJob(implicit configuration: ScoobiConfiguration) = (job: Job) => {

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
    job
  }

  private[scoobi] def collectOutputs(implicit configuration: ScoobiConfiguration) = (job: Job) => {
    /* Move named file-based sinks to their correct output paths. */
    reducers.collectOutputs(fileSystems.listPaths(configuration.temporaryOutputDirectory))
    configuration.deleteTemporaryOutputDirectory
    job
  }
}

case class KeyTypes(types: Map[Int, (WireFormat[_], Grouping[_])] = Map()) {
  def add(tag: Int, wf: WireFormat[_], gp: Grouping[_]) =
    copy(types = types + (tag -> (wf, gp)))
}
case class ValueTypes(types: Map[Int, Tuple1[WireFormat[_]]] = Map()) {
  def add(tag: Int, wf: WireFormat[_]) =
    copy(types = types + (tag -> Tuple1(wf)))
}
case class MscrMappers(mappers: Map[Source, Set[(Env[_], TaggedMapper)]] = Map().withDefaultValue(Set())) {
  def add(input: Source, env: Env[_], mapper: TaggedMapper) =
    copy(mappers = mappers + (input -> (mappers(input) + ((env, mapper)))))

  def sources = mappers.keys.toSeq
  def inputs  = Map(mappers.zipWithIndex.toSeq.map { case ((source, ms), i) => (i, (source.inputConverter, ms)) }:_*)

}
case class MscrReducers(reducers: Set[(Seq[Sink], (Env[_], TaggedReducer))] = Set()) {
  def add(sinks: Seq[Sink], env: Env[_], reducer: TaggedReducer) =
    copy(reducers = reducers + (sinks -> (env, reducer)))

  def sinks = reducers.flatMap(_._1)

  def configureBridgeStore(jar: JarBuilder) {
    sinks collect { case bs : BridgeStore[_] => jar.addRuntimeClass(bs.rtClass) }
  }

  def configureOutputChannel(job: Job)(implicit sc: ScoobiConfiguration) {
    reducers foreach { case (sinks, (_, reducer)) =>
      sinks.zipWithIndex.foreach { case (sink, i) =>
        ChannelOutputFormat.addOutputChannel(sink.configureCompression(job), reducer.tag, i, sink)
      }
    }
  }

  lazy val outputs = Map(reducers.toSeq.map { case (sinks, reducer) => (reducer._2.tag, (sinks.map(_.outputConverter).zipWithIndex.map(_.swap), reducer)) }:_*)

  def collectOutputs(outputFiles: Seq[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems) {
    val fs = configuration.fileSystem
    import fileSystems._

    reducers.foreach { case (sinks, (_, reducer)) =>
      sinks.zipWithIndex.foreach { case (sink, ix) =>
        sink.outputPath foreach { outDir =>
          fs.mkdirs(outDir)
          val files = outputFiles filter isResultFile(reducer.tag, ix)
          files foreach moveTo(outDir)
        }
      }
    }
  }
}

case class MscrCombiners(combiners: Set[TaggedCombiner[_]] = Set()) {
  def add(combiner: TaggedCombiner[_]) = copy(combiners = combiners + combiner)
  def isEmpty = combiners.isEmpty
  def combinersByTag: Map[Int, TaggedCombiner[_]] = Map(combiners.toSeq.map(tc => (tc.tag, tc)):_*)
}


object MapReduceJob {

  /** Construct a MapReduce job from an MSCR. */
  def create(stepId: Int, mscr: Mscr)(implicit configuration: ScoobiConfiguration): MapReduceJob =
    new MapReduceJob(stepId, mscr = mscr).configureChannels
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
          case KILLED    => logger.info(taskAttempt + " was killed!" + moreInfo)
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
