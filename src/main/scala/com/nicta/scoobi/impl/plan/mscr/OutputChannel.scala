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
package plan
package mscr

import core._
import comp._
import scalaz.Equal
import com.nicta.scoobi.impl.io.{Files, FileSystems}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.commons.logging.{Log, LogFactory}
import mapreducer._
import ChannelOutputFormat._
import monitor.Loggable._
import CollectFunctions._
import control.Functions._
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import com.nicta.scoobi.io.partition.PartitionedSink

/**
 * An OutputChannel is responsible for emitting key/values grouped by one Gbk or passed through from an InputChannel with no grouping
 *
 * Two OutputChannels are equal if they have the same tag. This tag is the id of the last processing node of the channel
 */
trait OutputChannel extends Channel {
  /** unique identifier for the Channel */
  def tag: Int

  /** sequence of the bridgeStore + additional sinks of the last node of the output channel */
  def sinks: Seq[Sink]

  /**
   * sequence of all the nodes which may require some input data to be loaded, like a ParallelDo used as a reducer and
   * needing its environment
   */
  def inputNodes: Seq[CompNode]
  /** output nodes for this channel */
  def outputNodes: Seq[CompNode]

  /** setup the nodes of the channel before writing data */
  def setup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)
  /** reduce key/values, given the current output format */
  def reduce(key: Any, values: Iterable[Any], channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)
  /** cleanup the channel, given the current output format */
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)

  /** copy all outputs files to the destinations specified by sink files */
  def collectOutputs(outputFiles: Seq[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems)

  /** copy the success file to the destinations specified by sink files */
  def collectSuccessFile(successFile: Option[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems)
}

/**
 * Implementation of an OutputChannel for a Mscr
 */
trait MscrOutputChannel extends OutputChannel { outer =>
  protected implicit lazy val logger = LogFactory.getLog("scoobi.OutputChannel")

  def graph: Graph
  lazy val graphNodes = graph

  override def equals(a: Any) = a match {
    case o: OutputChannel => o.tag == tag
    case _                => false
  }
  override def hashCode = tag.hashCode

  /** @return all the sinks defined by the nodes of the input channel */
  lazy val sinks: Seq[Sink] = {
    if (graph.uses(lastNode).forall(isRoot) && lastNode.sinks.size > 1) lastNode.sinks.filterNot(_ == lastNode.bridgeStore)
    else lastNode.sinks
  }

  /** @return last node of the channel to emit values */
  protected def lastNode: ProcessNode

  protected var emitter: EmitterWriter = _

  def setup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    graph.init
    logger.info("Outputs are " + sinks.map(_.outputPath(ScoobiConfiguration(configuration))).mkString("\n"))

    sinks.foreach(_.outputSetup(ScoobiConfiguration(configuration)))
    emitter = createEmitter(channelOutput)
  }

  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    sinks.foreach(_.outputTeardown(ScoobiConfiguration(configuration)))
  }

  /** copy all outputs files to the destinations specified by sink files */
  def collectOutputs(outputFiles: Seq[Path])(implicit sc: ScoobiConfiguration, fileSystems: FileSystems) {
    import fileSystems._; implicit val configuration = sc.configuration

    outer.logger.debug("output files are "+outputFiles.mkString("\n"))
    sinks.foreach { case sink =>
      sink.outputPath foreach { outDir =>
        mkdir(outDir)
        outer.logger.debug("created directory "+outDir)
      }
    }

    // copy the each result file to its sink
    sinks.foreach {
      case sink: PartitionedSink[_,_,_,_] =>
        sink.outputPath foreach { outDir =>
          // all directories are created under a <sink id> directory for easier collection in just a "rename"
          val baseDir = new Path(sc.temporaryOutputDirectory, new Path(sink.id.toString))
          outer.logger.debug(s"Partitioned sink. Moving the files found in $baseDir to $outDir. The files are:")
          fileSystems.listPaths(baseDir).foreach(p => outer.logger.debug(s" --> found $p"))
          moveTo(outDir)(sc.configuration)(baseDir, new Path("."))
        }

      case sink =>
        sink.outputPath foreach { outDir =>
          moveOutputFiles(sink, outDir, outputFiles)
        }
    }
  }

  def collectSuccessFile(successFile: Option[Path])(implicit sc: ScoobiConfiguration, fileSystems: FileSystems) = {
    import fileSystems._; implicit val configuration = sc.configuration

    // if the job is successful, create a success file to every output directory
    // however create it as a _SUCCESS_JOB file
    // this will be switched to a _SUCCESS job if all the jobs of the Scoobi job succeed
    successFile.foreach { successFile =>
      sinks.flatMap(_.outputPath).foreach { outDir =>
        FileSystems.fileSystem(outDir).createNewFile(new Path(outDir, "_SUCCESS_JOB"))
      }
    }
  }

  /**
   * move the files of a given sink to its output directory.
   */
  private def moveOutputFiles(sink: Sink, outDir: Path, outputFiles: Seq[Path])(implicit sc: ScoobiConfiguration, fileSystems: FileSystems) = {
    val outputs = outputFiles.filter(sink.isSinkResult(tag))
    outer.logger.debug("outputs result files for tag "+tag+" and sink id "+sink.id+" are "+outputs.map(_.getName).mkString("\n"))

    // move the directory containing the output files for a given tag and sink
    // and cleanup temporary files if any
    outputs.foreach { path =>
      OutputChannel.moveFileFromTo(srcDir = new Path(sc.temporaryOutputDirectory, path.getName), destDir = outDir).apply(path)
    }
  }

  /**
   * create an emitter to output values on the current tag for each sink. Values are converted to (key, values) using
   * the sink output converter. This emitter is used by both the GbkOutputChannel and the BypassOutputChannel
   */
  protected def createEmitter(channelOutput: ChannelOutputFormat)(implicit configuration1: Configuration) = new EmitterWriter with InputOutputContextScoobiJobContext {
    def write(x: Any)  {
      sinks foreach { sink =>
        sink.configureCompression(configuration1)
        channelOutput.write(tag, sink.id, convert(sink, x))
      }
    }
    def context = new InputOutputContext(channelOutput.context.asInstanceOf[TaskInputOutputContext[Any, Any, Any, Any]])
  }

  /** use the output converter of a sink to convert a value to a key/value */
  protected def convert(sink: Sink, x: Any)(implicit configuration: Configuration) = sink.outputConverter.asInstanceOf[ToKeyValueConverter].asKeyValue(x)

  /** create a ScoobiConfiguration from a Hadoop one */
  protected def scoobiConfiguration(configuration: Configuration): ScoobiConfiguration = ScoobiConfigurationImpl(configuration)

}

/**
 * Output channel for a GroupByKey.
 *
 * It can optionally have a reducer and / or a combiner applied to the grouped key/values.
 *
 * The possible combinations are
 *
 *   - gbk
 *   - gbk -> combiner
 *   - gbk -> reducer
 *   - gbk -> combiner -> reducer
 *
 * There can not be gbk -> reducer -> combiner because in that case the second combiner is transformed as a parallelDo
 * by the Optimiser
 */
case class GbkOutputChannel(groupByKey: GroupByKey,
                            combiner:   Option[Combine]    = None,
                            reducer:    Option[ParallelDo] = None,
                            graph: Graph = Graph(Root(Seq()))) extends MscrOutputChannel {

  /** the tag identifying a GbkOutputChannel is the groupByKey id */
  lazy val tag = groupByKey.id
  /** return the reducer environment if there is one */
  lazy val inputNodes = reducer.toSeq.map(_.env)
  /** output nodes for this channel */
  lazy val outputNodes: Seq[CompNode] = graph.uses(lastNode).toSeq

  /** store the reducer environment during the setup if there is one */
  protected var environment: Any = _
  protected implicit var scoobiConfiguration: ScoobiConfiguration = _

  /** only the reducer needs to be setup if there is one */
  override def setup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    super.setup(channelOutput)
    scoobiConfiguration = scoobiConfiguration(configuration)
    reducer.foreach { r =>
      environment = r.environment(scoobiConfiguration)
      r.setup(environment)(scoobiConfiguration)
    }
  }

  /**
   * reduce all the key/values with either the reducer, or the combiner
   * otherwise just emit key/value pairs.
   *
   * The key and values are untagged. The emitter is in charge of writing them to the proper tag, which is the channel's tag
   */
  def reduce(key: Any, values: Iterable[Any], channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    val combinedValues = combiner.flatMap(_.reduce(values, emitter.context)).getOrElse(values)
    reducer.fold(emitter.write((key, combinedValues)))(_.reduce(environment, key, combinedValues, emitter))
  }

  /** invoke the reducer cleanup if there is one */
  override def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    reducer.foreach(_.cleanup(environment, emitter))
    super.cleanup(channelOutput)
  }

  /** @return the last node of this channel */
  lazy val lastNode = reducer.orElse(combiner).getOrElse(groupByKey)

  override def toString =
    Seq(Some(groupByKey),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")

  def processNodes: Seq[ProcessNode] = Seq(groupByKey) ++ combiner.toSeq ++ reducer.toSeq
}

/**
 * This output channel simply copy values coming from a ParallelDo input (a mapper in an Input channel)
 * to this node sinks and bridgeStore
 */
case class BypassOutputChannel(input: ParallelDo, graph: Graph = Graph(Root(Seq()))) extends MscrOutputChannel {
  /** the tag identifying a BypassOutputChannel is the parallelDo id */
  lazy val tag = input.id
  /** @return the last node of this channel */
  lazy val lastNode = input
  /** return the environment of the input node */
  lazy val inputNodes = Seq(input.env)
  lazy val outputNodes = graph.uses(input).toSeq
  /**
   * Just emit the values to the sink, the key is irrelevant since it is a RollingInt in that case
   */
  def reduce(key: Any, values: Iterable[Any], channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    values foreach emitter.write
  }

  def processNodes: Seq[ProcessNode] = Seq()
}

/**
 * Utility functions for Output channels
 */
object OutputChannel {

  implicit def outputChannelEqual = new Equal[OutputChannel] {
    def equal(a1: OutputChannel, a2: OutputChannel) = a1.tag == a2.tag
  }

  /**
   * This function moves a file to the expected output directory,
   * keeping the subdirectories in which the file was created
   *
   * For example some files created with partitionedTextFileSink might have a path portion like
   *   year=2013/month=12/day=01/ch4out5-m-00000
   *
   */
  def moveFileFromTo(srcDir: Path, destDir: Path)(implicit sc: ScoobiConfiguration, fileSystems: FileSystems, outerLogger: Log): Path => Unit = { path =>
    import fileSystems._; implicit val configuration = sc.configuration

    val filePath       = if (isDirectory(path)) Files.dirPath(path.toUri.getPath) else path.toUri.getPath
    val sourceDirPath  = Files.dirPath(srcDir.toUri.getPath)
    // take only the path part which starts after the source directory
    val fromSourceDir  =
      if (filePath.indexOf(sourceDirPath) >=0) filePath.substring(filePath.indexOf(sourceDirPath)).replace(sourceDirPath, "")
      else filePath

    val newPath = if (fromSourceDir.isEmpty) new Path(".") else new Path(fromSourceDir)

    val moved = moveTo(destDir).apply(path, newPath)
    if (!moved) outerLogger.error(s"can not move \n $path to \n ${new Path(destDir, newPath)}")
  }
}
