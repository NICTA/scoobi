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
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.compress.CompressionCodec
import scala.collection.mutable.{Map => MMap}
import scala.math._
import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
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
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.impl.util.UniqueInt
import com.nicta.scoobi.impl.util.JarBuilder


/** A class that defines a single Hadoop MapReduce job. */
class MapReduceJob {

  import scala.collection.mutable.{Set => MSet, Map => MMap}

  /* Keep track of all the mappers for each input channel. */
  private val mappers: MMap[DataSource, MSet[TaggedMapper[_,_,_]]] = MMap.empty
  private val combiners: MSet[TaggedCombiner[_]] = MSet.empty
  private val reducers: MMap[List[_ <: DataSink], TaggedReducer[_,_,_]] = MMap.empty

  /* The types that will be combined together to form (K2, V2). */
  private val keyTypes: MMap[Int, (Manifest[_], WireFormat[_], Ordering[_])] = MMap.empty
  private val valueTypes: MMap[Int, (Manifest[_], WireFormat[_])] = MMap.empty


  /** Add an input mapping function to thie MapReduce job. */
  def addTaggedMapper[A, K, V](input: DataSource, m: TaggedMapper[A, K, V]): Unit = {
    if (!mappers.contains(input))
      mappers += (input -> MSet(m))
    else
      mappers(input) += m

    m.tags.foreach { tag =>
      keyTypes   += (tag -> (m.mK, m.wtK, m.ordK))
      valueTypes += (tag -> (m.mV, m.wtV))
    }
  }

  /** Add a combiner function to this MapReduce job. */
  def addTaggedCombiner[V](c: TaggedCombiner[V]): Unit = {
    combiners += c
  }

  /** Add an output reducing function to this MapReduce job. */
  def addTaggedReducer[K, V, B](outputs: Set[_ <: DataSink], r: TaggedReducer[K, V, B]): Unit = {
    reducers += (outputs.toList -> r)
  }

  /** Take this MapReduce job and run it on Hadoop. */
  def run() = {

    val job = new Job(Scoobi.conf, Scoobi.jobId)
    val fs = FileSystem.get(job.getConfiguration)

    /* Job output always goes to temporary dir from which files are subsequently moved from
     * once the job is finished. */
    val tmpOutputPath = new Path(Scoobi.getWorkingDirectory(job.getConfiguration), "tmp-out")

    /** Make temporary JAR file for this job. At a minimum need the Scala runtime
      * JAR, the Scoobi JAR, and the user's application code JAR(s). */
    val tmpFile = File.createTempFile("scoobi-job-", ".jar")
    var jar = new JarBuilder(tmpFile.getAbsolutePath)
    job.getConfiguration.set("mapred.jar", tmpFile.getAbsolutePath)

    jar.addContainingJar(classOf[List[_]])        //  Scala
    jar.addContainingJar(this.getClass)           //  Scoobi
    Scoobi.getUserJars.foreach { jar.addJar(_) }  //  User JARs


    /** (K2,V2):
      *   - are (TaggedKey, TaggedValue), the wrappers for all K-V types
      *   - generate their runtime classes and add to JAR */
    val id = UniqueId.get
    val tkRtClass = TaggedKey("TK" + id, keyTypes.toMap)
    val tvRtClass = TaggedValue("TV" + id, valueTypes.toMap)

    jar.addRuntimeClass(tkRtClass)
    jar.addRuntimeClass(tvRtClass)

    job.setMapOutputKeyClass(tkRtClass.clazz)
    job.setMapOutputValueClass(tvRtClass.clazz)
    job.setPartitionerClass(classOf[TaggedPartitioner])


    /** Mappers:
      *     - generate runtime class (ScoobiWritable) for each input value type and add to JAR (any
      *       mapper for a given input channel can be used as they all have the same input type
      *     - use ChannelInputs to specify multiple mappers through job */
    val inputChannels = mappers.toList.zipWithIndex
    inputChannels.foreach { case ((input, ms), ix) =>
      val mapper = ms.head
      val valRtClass = ScoobiWritable(input.inputTypeName, mapper.mA, mapper.wtA)
      jar.addRuntimeClass(valRtClass)
      ChannelInputFormat.addInputChannel(job,
                                         ix,
                                         input.inputPath,
                                         input.inputFormat.asInstanceOf[Class[_ <: FileInputFormat[_,_]]])
    }

    DistCache.pushObject(job.getConfiguration, inputChannels map { case((_, ms), ix) => (ix, ms.toSet) } toMap, "scoobi.input.mappers")
    job.setMapperClass(classOf[MscrMapper[_,_,_]].asInstanceOf[Class[_ <: Mapper[_,_,_,_]]])


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
      *     - add a named output for each output channel
      *     - generate runtime class (ScoobiWritable) for each output value type and add to JAR */
    FileOutputFormat.setOutputPath(job, tmpOutputPath)
    reducers.foreach { case (outputs, reducer) =>
      val valRtClass = ScoobiWritable(outputs.head.outputTypeName, reducer.mB, reducer.wtB)
      jar.addRuntimeClass(valRtClass)
      outputs.zipWithIndex.foreach { case (out, ix) =>
        ChannelOutputFormat.addOutputChannel(job,
                                             reducer.tag,
                                             ix,
                                             out.outputFormat.asInstanceOf[Class[_ <: OutputFormat[_,_]]],
                                             valRtClass.clazz)
      }
    }

    DistCache.pushObject(job.getConfiguration, reducers map { case (os, tr) => (tr.tag, (os.size, tr)) } toMap, "scoobi.output.reducers")
    job.setReducerClass(classOf[MscrReducer[_,_,_]].asInstanceOf[Class[_ <: Reducer[_,_,_,_]]])


    /* Calculate the number of reducers to use with a simple heuristic:
     *
     * At a minimum, have at least as many reduce tasks as there are output channels. Then, base the
     * amount of parallelism required in the reduce phase on the size of the data output. Further,
     * estimate the size of output data to be the size of the input data to the MapReduce job. Then, set
     * the number of reduce tasks to the number of 1GB data chunks in the estimated output. */
    val inputBytes: Long = mappers.toIterable.flatMap {
      case (src, _) => fs.globStatus(src.inputPath).map(p => fs.getContentSummary(p.getPath).getLength)
    }.sum
    val inputGigabytes: Int = (inputBytes / (1000 * 1000 * 1000)).toInt + 1
    job.setNumReduceTasks(inputGigabytes)
    job.setNumReduceTasks(max(inputGigabytes, reducers.size))


    /* Run job then tidy-up. */
    jar.close()
    job.waitForCompletion(true)
    tmpFile.delete


    /* Move named outputs to the correct directories */
    val outputFiles = fs.listStatus(tmpOutputPath) map { _.getPath }
    val FileName = """ch(\d+)out(\d+)-.-\d+""".r

    reducers.foreach { case (outputs, reducer) =>

      outputs.zipWithIndex.foreach { case (output, ix) =>
        outputFiles filter (forOutput) foreach { srcPath =>
          fs.mkdirs(output.outputPath)
          fs.rename(srcPath, new Path(output.outputPath, srcPath.getName))
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
  def apply(mscr: MSCR): MapReduceJob = {
    val job = new MapReduceJob
    val mapperTags: MMap[AST.Node[_], Set[Int]] = MMap.empty

    /* Tag each output channel with a unique index. */
    mscr.outputChannels.zipWithIndex.foreach { case (oc, tag) =>

      def addTag(n: AST.Node[_], tag: Int): Unit = {
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
        case GbkOutputChannel(_, _, _, JustCombiner(c))       => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case GbkOutputChannel(_, _, _, CombinerReducer(c, _)) => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case _                                                => Unit
      }

      /* Add reducer functionality from output channel descriptions. */
      oc match {
        case GbkOutputChannel(outputs, _, _, JustCombiner(c))       => job.addTaggedReducer(outputs, c.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, _, JustReducer(r))        => job.addTaggedReducer(outputs, r.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, _, CombinerReducer(_, r)) => job.addTaggedReducer(outputs, r.mkTaggedReducer(tag))
        case GbkOutputChannel(outputs, _, g, Empty)                 => job.addTaggedReducer(outputs, g.mkTaggedReducer(tag))
        case BypassOutputChannel(outputs, origin)                   => job.addTaggedReducer(outputs, origin.mkTaggedReducer(tag))
        case FlattenOutputChannel(outputs, flat)                    => job.addTaggedReducer(outputs, flat.mkTaggedReducer(tag))
      }
    }

    /* Add mapping functionality from input channel descriptions. */
    mscr.inputChannels.foreach { ic =>
      ic match {
        case b@BypassInputChannel(input, origin) => {
          job.addTaggedMapper(input, origin.mkTaggedIdentityMapper(mapperTags(origin)))
        }
        case MapperInputChannel(input, mappers) => mappers.foreach { m =>
          job.addTaggedMapper(input, m.mkTaggedMapper(mapperTags(m)))
        }
        case StraightInputChannel(input, origin) =>
          job.addTaggedMapper(input, origin.mkStraightTaggedIdentityMapper(mapperTags(origin)))
      }
    }

    job
  }
}

object UniqueId extends UniqueInt
