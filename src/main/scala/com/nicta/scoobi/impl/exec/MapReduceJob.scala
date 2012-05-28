/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.exec

import java.io.File
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.RawComparator
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{MutableList=> MList}
import Option.{apply => ?}

import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.impl.plan.Shape
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.MapperInputChannel
import com.nicta.scoobi.impl.plan.BypassInputChannel
import com.nicta.scoobi.impl.plan.StraightInputChannel
import com.nicta.scoobi.impl.plan.GbkOutputChannel
import com.nicta.scoobi.impl.plan.BypassOutputChannel
import com.nicta.scoobi.impl.plan.FlattenOutputChannel
import com.nicta.scoobi.impl.plan.MSCR
import com.nicta.scoobi.impl.plan.Empty
import com.nicta.scoobi.impl.plan.JustCombiner
import com.nicta.scoobi.impl.plan.JustReducer
import com.nicta.scoobi.impl.plan.CombinerReducer
import com.nicta.scoobi.impl.rtt.TaggedKey
import com.nicta.scoobi.impl.rtt.TaggedValue
import com.nicta.scoobi.impl.rtt.TaggedPartitioner
import com.nicta.scoobi.impl.rtt.TaggedGroupingComparator
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.util.UniqueInt
import com.nicta.scoobi.impl.util.JarBuilder
import com.nicta.scoobi.{ScoobiConfiguration, Scoobi, WireFormat, Grouping}
import ScoobiConfiguration._
import com.nicta.scoobi.impl.plan._

/** A class that defines a single Hadoop MapReduce job. */
class MapReduceJob(stepId: Int) {
  lazy val logger = LogFactory.getLog("scoobi.Step")

  import scala.collection.mutable.{Set => MSet, Map => MMap}

  /* Keep track of all the mappers for each input channel. */
  private val mappers: MMap[DataSource[_,_,_], MSet[(Env[_], TaggedMapper[_,_,_,_])]] = MMap.empty
  private val combiners: MSet[TaggedCombiner[_]] = MSet.empty
  private val reducers: MList[(List[_ <: DataSink[_,_,_]], (Env[_], TaggedReducer[_,_,_,_]))] = MList.empty

  /* The types that will be combined together to form (K2, V2). */
  private val keyTypes: MMap[Int, (Manifest[_], WireFormat[_], Grouping[_])] = MMap.empty
  private val valueTypes: MMap[Int, (Manifest[_], WireFormat[_])] = MMap.empty


  /** Add an input mapping function to thie MapReduce job. */
  def addTaggedMapper(input: DataSource[_,_,_], env: Option[Env[_]], m: TaggedMapper[_,_,_,_]) {
    val tm = env match {
      case Some(e) => (e, m)
      case None    => (Env.empty, m)
    }

    if (!mappers.contains(input))
      mappers += (input -> MSet(tm))
    else
      mappers(input) += tm

    m.tags.foreach { tag =>
      keyTypes   += (tag -> (m.mK, m.wtK, m.grpK))
      valueTypes += (tag -> (m.mV, m.wtV))
    }
  }

  /** Add a combiner function to this MapReduce job. */
  def addTaggedCombiner[V](c: TaggedCombiner[V]) {
    combiners += c
  }

  /** Add an output reducing function to this MapReduce job. */
  def addTaggedReducer(outputs: Set[_ <: DataSink[_,_,_]], env: Option[Env[_]], r: TaggedReducer[_,_,_,_]) {
    val tr = env match {
      case Some(e) => (e, r)
      case None    => (Env.empty, r)
    }

    reducers += (outputs.toList -> tr)
  }

  /** Take this MapReduce job and run it on Hadoop. */
  def run(implicit configuration: ScoobiConfiguration) = {

    val job = new Job(configuration, configuration.jobId + "(Step-" + stepId + ")")
    val fs = FileSystem.get(job.getConfiguration)

    /* Job output always goes to temporary dir from which files are subsequently moved from
     * once the job is finished. */
    val tmpOutputPath = new Path(configuration.workingDirectory, "tmp-out")

    /** Make temporary JAR file for this job. At a minimum need the Scala runtime
      * JAR, the Scoobi JAR, and the user's application code JAR(s). */
    val tmpFile = File.createTempFile("scoobi-job-"+configuration.jobId, ".jar")
    var jar = new JarBuilder(tmpFile.getAbsolutePath)
    job.getConfiguration.set("mapred.jar", tmpFile.getAbsolutePath)

    jar.addContainingJar(this.getClass)                          //  Scoobi
    configuration.userJars.foreach { jar.addJar(_) }             //  User JARs
    configuration.userDirs.foreach { jar.addClassDirectory(_) }

    /** Sort-and-shuffle:
      *   - (K2, V2) are (TaggedKey, TaggedValue), the wrappers for all K-V types
      *   - Partitioner is generated and of type TaggedPartitioner
      *   - GroupingComparator is generated and of type TaggedGroupingComparator
      *   - SortComparator is handled by TaggedKey which is WritableComparable */
    val id = UniqueId.get

    val tkRtClass = TaggedKey("TK" + id, keyTypes.toMap)
    jar.addRuntimeClass(tkRtClass)
    job.setMapOutputKeyClass(tkRtClass.clazz)

    val tvRtClass = TaggedValue("TV" + id, valueTypes.toMap)
    jar.addRuntimeClass(tvRtClass)
    job.setMapOutputValueClass(tvRtClass.clazz)

    val tpRtClass = TaggedPartitioner("TP" + id, keyTypes.toMap)
    jar.addRuntimeClass(tpRtClass)
    job.setPartitionerClass(tpRtClass.clazz.asInstanceOf[Class[_ <: Partitioner[_,_]]])

    val tgRtClass = TaggedGroupingComparator("TG" + id, keyTypes.toMap)
    jar.addRuntimeClass(tgRtClass)
    job.setGroupingComparatorClass(tgRtClass.clazz.asInstanceOf[Class[_ <: RawComparator[_]]])


    /** Mappers:
      *     - use ChannelInputs to specify multiple mappers through job
      *     - generate runtime class (ScoobiWritable) for each input value type and add to JAR (any
      *       mapper for a given input channel can be used as they all have the same input type */
    ChannelsInputFormat.configureSources(job, jar, mappers.keys.toList)

    val inputChannels: List[((DataSource[_,_,_], MSet[(Env[_], TaggedMapper[_,_,_,_])]), Int)] = mappers.toList.zipWithIndex
    val inputs: Map[Int, (InputConverter[_, _, _], Set[(Env[_], TaggedMapper[_,_,_,_])])] =
      inputChannels map { case((source, ms), ix) => (ix, (source.inputConverter, ms.toSet)) } toMap

    DistCache.pushObject(job.getConfiguration, inputs, "scoobi.mappers")
    job.setMapperClass(classOf[MscrMapper[_,_,_,_,_,_]].asInstanceOf[Class[_ <: Mapper[_,_,_,_]]])


    /** Combiners:
      *   - only need to make use of Hadoop's combiner facility if actual combiner
      *   functions have been added
      *   - use distributed cache to push all combine code out */
    if (!combiners.isEmpty) {
      val combinerMap: Map[Int, TaggedCombiner[_]] = combiners.map(tc => (tc.tag, tc)).toMap
      DistCache.pushObject(job.getConfiguration, combinerMap, "scoobi.combiners")
      job.setCombinerClass(classOf[MscrCombiner[_]].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])
    }


    /** Reducers:
      *     - generate runtime class (ScoobiWritable) for each output values being written to
      *       a BridgeStore or MaterializeStore and add to JAR
      *     - add a named output for each output channel */
    FileOutputFormat.setOutputPath(job, tmpOutputPath)
    reducers.foreach { case (sinks, (_, reducer)) =>
      sinks foreach {
        case bs@BridgeStore() => {
          // TODO - really want to be doing this inside the BridgeStore class (like MaterializeStore)
          bs.rtClass match {
            case Some(rtc) => jar.addRuntimeClass(rtc)
            case None      => {
              /* NOTE: must do this before calling addOutputChannel */
              val rtClass = ScoobiWritable(bs.typeName, reducer.mB, reducer.wtB)
              jar.addRuntimeClass(rtClass)
              bs.rtClass = Some(rtClass)
            }
          }
        }
        case ms@MaterializeStore(_, _) => jar.addRuntimeClass(ms.rtClass)
        case _ => {}
      }
      sinks.zipWithIndex.foreach { case (sink, ix) => ChannelOutputFormat.addOutputChannel(job, reducer.tag, ix, sink) }
    }

    val outputs: Map[Int, (List[(Int, OutputConverter[_,_,_])], (Env[_], TaggedReducer[_,_,_,_]))] =
      reducers map { case (sinks, reducer) =>
        (reducer._2.tag, (sinks.map(_.outputConverter).zipWithIndex.map(_.swap), reducer))
      } toMap

    DistCache.pushObject(job.getConfiguration, outputs, "scoobi.reducers")
    job.setReducerClass(classOf[MscrReducer[_,_,_,_,_,_]].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])


    /* Calculate the number of reducers to use with a simple heuristic:
     *
     * Base the amount of parallelism required in the reduce phase on the size of the data output. Further,
     * estimate the size of output data to be the size of the input data to the MapReduce job. Then, set
     * the number of reduce tasks to the number of 1GB data chunks in the estimated output. */
    val inputBytes: Long = mappers.keys.map(_.inputSize()).sum
    val inputGigabytes: Int = (inputBytes / (1000 * 1000 * 1000)).toInt + 1
    val numReducers: Int = inputGigabytes.toInt
    job.setNumReduceTasks(numReducers)


    /* Log stats on this MR job. */
    logger.info("Total input size: " +  Helper.sizeString(inputBytes))
    logger.info("Number of reducers: " + numReducers)

    try {

      /* Run job */
      jar.close(configuration)
      job.submit()

      val map = new Progress(job.mapProgress())
      val reduce = new Progress(job.reduceProgress())

      while (!job.isComplete) {
        Thread.sleep(configuration.getInt("scoobi.progress.time", 5000))
        if (map.hasProgressed || reduce.hasProgressed)
          logger.info("Map " + map.getProgress.formatted("%3d") + "%    " +
                      "Reduce " + reduce.getProgress.formatted("%3d") + "%")
      }
    } finally {
      /* Tidy-up */
      tmpFile.delete
    }

    /* Move named file-based sinks to their correct output paths. */
    val outputFiles = fs.listStatus(tmpOutputPath) map { _.getPath }
    val FileName = """ch(\d+)out(\d+)-.-\d+.*""".r

    reducers.foreach { case (sinks, (_, reducer)) =>

      sinks.zipWithIndex.foreach { case (sink, ix) =>
        outputFiles filter (forOutput) foreach { srcPath =>
          val outputPath = {
            val jobCopy = new Job(job.getConfiguration)
            sink.outputConfigure(jobCopy)
            FileOutputFormat.getOutputPath(jobCopy)
          }
          ?(outputPath) foreach { p =>
            fs.mkdirs(p)
            fs.rename(srcPath, new Path(p, srcPath.getName))
          }
        }

        def forOutput = (f: Path) => f.getName match {
          case FileName(t, i) => t.toInt == reducer.tag && i.toInt == ix
          case _              => false
        }

      }
    }

    fs.delete(tmpOutputPath, true)
  }
}


object MapReduceJob {

  /** Construct a MapReduce job from an MSCR. */
  def apply(stepId: Int, mscr: MSCR): MapReduceJob = {
    val job = new MapReduceJob(stepId)
    val mapperTags: MMap[AST.Node[_, _ <: Shape], Set[Int]] = MMap.empty

    /* Tag each output channel with a unique index. */
    mscr.outputChannels.zipWithIndex.foreach { case (oc, tag) =>

      def addTag(n: AST.Node[_, _ <: Shape], tag: Int) = {
        val s = mapperTags.getOrElse(n, Set())
        mapperTags += (n -> (s + tag))
      }

      /* Build up a map of mappers to output channel tags. */
      oc match {
        case GbkOutputChannel(_, Some(AST.Flatten(ins)), _, _)  => ins.foreach { in => addTag(in, tag) }
        case GbkOutputChannel(_, None, AST.GroupByKey(in), _)   => addTag(in, tag)
        case BypassOutputChannel(_, origin)                     => addTag(origin, tag)
        case FlattenOutputChannel(_, flat)                      => flat.ins.foreach { in => addTag(in, tag) }
      }

      /* Add combiner functionality from output channel descriptions. */
      oc match {
        case GbkOutputChannel(_, _, _, JustCombiner(c))          => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case GbkOutputChannel(_, _, _, CombinerReducer(c, _, _)) => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case _                                                   => Unit
      }

      /* Add reducer functionality from output channel descriptions. */
      oc match {
        case GbkOutputChannel(outputs, _, _, JustCombiner(c))            => job.addTaggedReducer(outputs, None, c.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, _, JustReducer(r, env))        => job.addTaggedReducer(outputs, Some(env), r.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, _, CombinerReducer(_, r, env)) => job.addTaggedReducer(outputs, Some(env), r.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, g, Empty)                      => job.addTaggedReducer(outputs, None, g.mkTaggedReducer(tag))
        case BypassOutputChannel(outputs, origin)                        => job.addTaggedReducer(outputs, None, origin.mkTaggedReducer(tag))
        case FlattenOutputChannel(outputs, flat)                         => job.addTaggedReducer(outputs, None, flat.mkTaggedReducer(tag))
      }
    }

    /* Add mapping functionality from input channel descriptions. */
    mscr.inputChannels.foreach { ic =>
      ic match {
        case b@BypassInputChannel(input, origin) => {
          job.addTaggedMapper(input, None, origin.mkTaggedIdentityMapper(mapperTags(origin)))
        }
        case MapperInputChannel(input, mappers) => mappers.foreach { case (env, m) =>
          job.addTaggedMapper(input, Some(env), m.mkTaggedMapper(mapperTags(m)))
        }
        case StraightInputChannel(input, origin) =>
          job.addTaggedMapper(input, None, origin.mkStraightTaggedIdentityMapper(mapperTags(origin)))
      }
    }

    job
  }
}


object UniqueId extends UniqueInt


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
