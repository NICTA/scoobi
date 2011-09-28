/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io.File
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.Partitioner
import org.apache.hadoop.mapred.lib.MultipleOutputs
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.GzipCodec
import scala.collection.mutable.{Map => MMap}


/* An input to a MapReduce job. */
sealed trait InputLike {
  def inputTypeName: String
  def inputPath: Path
  def inputFormat: Class[_ <: FileInputFormat[_,_]]
}


/* An output from a MapReduce job. */
sealed trait OutputLike {
  def outputTypeName: String
  def outputPath: Path
  def outputFormat: Class[_ <: FileOutputFormat[_,_]]
}


/** A connector is a node that is external to an MSCRs. As a consequence it is
  * external to a Hadoop job which means it must be perisisted somewhere, at least
  * temporarily, between jobs. There are three kinds: Inputs, Outputs and
  * Intermediates. */
sealed abstract class DConnector(val node: AST.Node[_]) {

  /* The name to be given to the type of this connector. Two different types can
   * not share the same name. */
  val typeName: String = "V" + UniqueId.get
}

object UniqueId extends UniqueInt


/** An input connector is synonomous with a 'Load' node. It already exists and
  * must persist. */
case class DInput(n: AST.Load, val path: String)
  extends DConnector(n)
  with InputLike {

  def inputTypeName = typeName
  val inputPath = new Path(path)
  val inputFormat = classOf[SimplerTextInputFormat]
}

object DInput {
  def apply(n: AST.Load) = new DInput(n, n.path)
}


/** An output connector is a node that must first be computed. Once computed it
  * must persist. Note that it is possible for an output to be an input to an
  * MSCR. */
case class DOutput(n: AST.Node[_], val path: String)
  extends DConnector(n)
  with InputLike
  with OutputLike {

  def inputTypeName = typeName
  val inputPath = new Path(path, "channel*")
  val inputFormat = classOf[SequenceFileInputFormat[_,_]]

  def outputTypeName = typeName
  val outputPath = new Path(path)
  val outputFormat = classOf[SequenceFileOutputFormat[_,_]]
}


/** An intermediate connector is any external MSCR node (i.e. a connector) that is
  * not an Input or Output. It must first be computed, but may be removed once
  * all successor nodes have consumed it. */
case class DIntermediate(n: AST.Node[_], val path: Path, val refCnt: Int)
  extends DConnector(n)
  with InputLike
  with OutputLike {

  def inputTypeName = typeName
  val inputPath = new Path(path, "channel*")
  val inputFormat = classOf[SequenceFileInputFormat[_,_]]

  def outputTypeName = typeName
  val outputPath = path
  val outputFormat = classOf[SequenceFileOutputFormat[_,_]]

  /** Free up the disk space being taken up by this intermediate data. */
  def freePath: Unit = {
    val fs = outputPath.getFileSystem(new JobConf)
    fs.delete(outputPath)
  }
}

/** Companion object for automating the creation of random temporary paths as the
  * location for storing intermediate data. */
object DIntermediate {

  private val conf = new JobConf
  private object TmpId extends UniqueInt

  def apply(node: AST.Node[_], refCnt: Int): DIntermediate = {
    val tmpPath = new Path(Scoobi.getWorkingDirectory(conf), "intermediates/" + TmpId.get.toString)
    DIntermediate(node, tmpPath, refCnt)
  }
}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



/** A class that defines a single Hadoop MapRedcue job. */
class MapReduceJob {

  import scala.collection.mutable.{Set => MSet, Map => MMap}

  /* Keep track of all the mappers for each input channel. */
  private val mappers: MMap[InputLike, MSet[TaggedMapper[_,_,_]]] = MMap.empty
  private val combiners: MSet[TaggedCombiner[_]] = MSet.empty
  private val reducers: MMap[OutputLike, TaggedReducer[_,_,_]] = MMap.empty

  /* The types that will be combined together to form (K2, V2). */
  private val keyTypes: MMap[Int, (Manifest[_], HadoopWritable[_], Ordering[_])] = MMap.empty
  private val valueTypes: MMap[Int, (Manifest[_], HadoopWritable[_])] = MMap.empty


  /** Add an input mapping function to thie MapReduce job. */
  def addTaggedMapper[A, K, V](input: InputLike, m: TaggedMapper[A, K, V]): Unit = {
    if (!mappers.contains(input))
      mappers += (input -> MSet(m))
    else
      mappers(input) += m

    keyTypes   += (m.tag -> (m.mK, m.wtK, m.ordK))
    valueTypes += (m.tag -> (m.mV, m.wtV))
  }

  /** Add a combiner function to this MapReduce job. */
  def addTaggedCombiner[V](c: TaggedCombiner[V]): Unit = {
    combiners += c
  }

  /** Add an output reducing function to this MapReduce job. */
  def addTaggedReducer[K, V, B](output: OutputLike, r: TaggedReducer[K, V, B]): Unit = {
    reducers += (output -> r)
  }

  /** Take this MapReduce job and run it on Hadoop. */
  def run() = {

    val conf = new Configuration
    val jobConf = new JobConf(conf)
    jobConf.setJobName("scoobi-job")

    /* Job output always goes to temporary dir from which files are subsequently moved from
     * once the job is finished. */
    val tmpOutputPath = new Path(Scoobi.getWorkingDirectory(jobConf), "tmp-out")

    /** Make temporary JAR file for this job. At a minimum need the Scala runtime
      * JAR, the Scoobi JAR, and the user's application code JAR(s). */
    val tmpFile = File.createTempFile("scoobi-job-", ".jar")
    var jar = new JarBuilder(tmpFile.getName)
    jobConf.setJar(jar.name)

    jar.addContainingJar(classOf[List[_]])        //  Scala
    jar.addContainingJar(this.getClass)           //  Scoobi
    Scoobi.getUserJars.foreach { jar.addJar(_) }  //  User JARs


    /** (K2,V2):
      *   - are (TaggedKey, TaggedValue), the wrappers for all K-V types
      *   - generate their runtime classes and add to JAR */
    val id = UniqueId.get
    val tkRtClass = TaggedKey("TK" + id, keyTypes.toMap)
    val tvRtClass = TaggedValue("TV" + id, valueTypes.toMap)
    val tpRtClass = TaggedPartitioner("TP" + id, keyTypes.size)


    jar.addRuntimeClass(tkRtClass)
    jar.addRuntimeClass(tvRtClass)
    jar.addRuntimeClass(tpRtClass)

    jobConf.setMapOutputKeyClass(tkRtClass.clazz)
    jobConf.setMapOutputValueClass(tvRtClass.clazz)
    jobConf.setPartitionerClass(tpRtClass.clazz.asInstanceOf[Class[_ <: Partitioner[_,_]]])


    /** Mappers:
      *     - generate runtime class (ScoobiWritable) for each input value type and add to JAR (any
      *       mapper for a given input channel can be used as they all have the same input type
      *     - use ChannelInputs to specify multiple mappers through jobconf */
    val inputChannels = mappers.toList.zipWithIndex
    inputChannels.foreach { case ((input, ms), ix) =>
      val mapper = ms.head
      val valRtClass = ScoobiWritable(input.inputTypeName, mapper.mA, mapper.wtA)
      jar.addRuntimeClass(valRtClass)
      ChannelInputFormat.addInputChannel(jobConf, ix, input.inputPath, input.inputFormat)
    }

    DistributedObject.pushObject(jobConf, inputChannels map { case((_, ms), ix) => (ix, ms.toSet) } toMap, "scoobi.input.mappers")
    jobConf.setMapperClass(classOf[MscrMapper[_,_,_]])


    /** Combiners:
      *   - only need to make use of Hadoop's combiner facility if actual combiner
      *   functions have been added
      *   - use distributed cache to push all combine code out */
    if (!combiners.isEmpty) {
      val combinerMap: Map[Int, TaggedCombiner[_]] = combiners.map(tc => (tc.tag, tc)).toMap
      DistributedObject.pushObject(jobConf, combinerMap, "scoobi.combiners")
      jobConf.setCombinerClass(classOf[MscrCombiner[_]])
    }


    /** Reducers:
      *     - add a named output for each output channel
      *     - generate runtime class (ScoobiWritable) for each output value type and add to JAR */
    FileOutputFormat.setOutputPath(jobConf, tmpOutputPath)
    reducers.foreach { case (output, reducer) =>
      val valRtClass = ScoobiWritable(output.outputTypeName, reducer.mB, reducer.wtB)
      jar.addRuntimeClass(valRtClass)
      MultipleOutputs.addNamedOutput(jobConf, "channel" + reducer.tag, output.outputFormat,
                                     classOf[NullWritable], valRtClass.clazz)
    }

    DistributedObject.pushObject(jobConf, reducers map { case (_, tr) => (tr.tag, tr) } toMap, "scoobi.output.reducers")
    jobConf.setReducerClass(classOf[MscrReducer[_,_,_]])


    /* Compress all sequence file output. (TODO - does it apply to all output?) */
//  FileOutputFormat.setCompressOutput(jobConf, true)
//  FileOutputFormat.setOutputCompressorClass(jobConf, classOf[GzipCodec])
//  SequenceFileOutputFormat.setOutputCompressionType(jobConf, SequenceFile.CompressionType.BLOCK)


    /* Run job then tidy-up. */
    jar.close()
    JobClient.runJob(jobConf)
    tmpFile.delete  // TODO - is not being deleted, why?

    /* Move named outputs to the correct directories */
    val fs = FileSystem.get(jobConf)
    val outputFiles = fs.listStatus(tmpOutputPath) map { _.getPath }
    val FileName = """channel(\d+)-.-\d+""".r

    reducers.foreach { case (output, reducer) =>
      outputFiles filter (forChannel) foreach { srcPath =>
        fs.mkdirs(output.outputPath)
        fs.rename(srcPath, new Path(output.outputPath, srcPath.getName))
      }

      def forChannel = (f: Path) => f.getName match {
        case FileName(n) => n.toInt == reducer.tag
        case _           => false
      }
    }

    fs.delete(tmpOutputPath, true)
  }
}


object MapReduceJob {

  /** Construct a MapReduce job from an MSCR. */
  def apply(mscr: MSCR): MapReduceJob = {
    val job = new MapReduceJob
    val mapperTags: MMap[AST.Node[_], Int] = MMap.empty

    /* Tag each output channel with a unique index. */
    mscr.outputChannels.zipWithIndex.foreach { case (oc, tag) =>

      /* Build up a map of mappers to output channel tags. */
      oc match {
        case GbkOutputChannel(_, Some(AST.Flatten(ins)), _, _)  => ins.foreach { in => mapperTags += (in -> tag) }
        case GbkOutputChannel(_, None, AST.GroupByKey(in), _)   => mapperTags += (in -> tag)
        case BypassOutputChannel(_, origin)                     => mapperTags += (origin -> tag)
      }

      /* Add combiner functionality from output channel descriptions. */
      oc match {
        case GbkOutputChannel(_, _, _, JustCombiner(c))       => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case GbkOutputChannel(_, _, _, CombinerReducer(c, _)) => job.addTaggedCombiner(c.mkTaggedCombiner(tag))
        case _                                                => Unit
      }

      /* Add reducer functionality from output channel descriptions. */
      oc match {
        case GbkOutputChannel(output, _, _, JustCombiner(c))       => job.addTaggedReducer(output, c.mkTaggedReducer(tag))
        case GbkOutputChannel(output, _, _, JustReducer(r))        => job.addTaggedReducer(output, r.mkTaggedReducer(tag))
        case GbkOutputChannel(output, _, _, CombinerReducer(_, r)) => job.addTaggedReducer(output, r.mkTaggedReducer(tag))
        case BypassOutputChannel(output, _)                        => job.addTaggedReducer(output, new TaggedIdentityReducer(tag))
        case _                                                     => Unit
      }
    }

    /* Add mapping functionality from input channel descriptions. */
    mscr.inputChannels.foreach { ic =>
      ic match {
        case b@BypassInputChannel(input, _) => {
          job.addTaggedMapper(input, new TaggedIdentityMapper(mapperTags(input.node))
                                                             (b.mK, b.wtK, b.ordK, b.mV, b.wtV, b.mKV, b.wtKV))
        }
        case MapperInputChannel(input, mappers) => mappers.foreach { m =>
          job.addTaggedMapper(input, m.mkTaggedMapper(mapperTags(m)))
        }
      }
    }

    job
  }
}
