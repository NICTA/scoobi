package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._
import util._
import scalaz.Equal
import impl.io.FileSystems
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import rtt.TaggedKey
import mapreducer.{ChannelOutputFormat, UntaggedValues}
import ChannelOutputFormat._

/**
 * An OutputChannel is responsible for emitting key/values grouped by one Gbk or passed through from an InputChannel with no grouping
 *
 * Two OutputChannels are equal if they have the same tag. This tag is the id of the last processing node of the channel
 */
trait OutputChannel {
  /** unique identifier for the Channel */
  def tag: Int

  /** an output channel write data to a bridge */
  def bridgeStore: Bridge
  /** sequence of the bridgeStore + all additional sinks */
  def sinks: Seq[Sink]

  /**
   * sequence of all the nodes which may require some input data to be loaded, like a ParallelDo used as a reducer and
   * needing its environment
   */
  def inputNodes: Seq[CompNode]

  /** setup the nodes of the channel before writing data */
  def setup(implicit configuration: Configuration)
  /** reduce key/values, given the current output format */
  def reduce(key: TaggedKey, untaggedValues: UntaggedValues, channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)
  /** cleanup the channel, given the current output format */
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)

  /** copy all outputs files to the destinations specified by sink files */
  def collectOutputs(outputFiles: Seq[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems)
}

/**
 * Implementation of an OutputChannel for a Mscr
 */
trait MscrOutputChannel extends OutputChannel {

  override def equals(a: Any) = a match {
    case o: OutputChannel => o.tag == tag
    case _                => false
  }
  override def hashCode = tag.hashCode

  /** @return the bridgeStore + all the sinks defined by the nodes of the input channel */
  def sinks = bridgeStore +: nodeSinks
  /** list of all the sinks defined by the channel nodes */
  protected def nodeSinks: Seq[Sink]

  /** copy all outputs files to the destinations specified by sink files */
  def collectOutputs(outputFiles: Seq[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems) {
    val fs = configuration.fileSystem
    import fileSystems._

    sinks.foreach { sink =>
      sink.outputPath foreach { outDir =>
        fs.mkdirs(outDir)
        val files = outputFiles filter isResultFile(tag, sink.id)
        files foreach moveTo(outDir)
      }
    }
  }

}

case class GbkOutputChannel(groupByKey:   GroupByKey,
                            combiner: Option[Combine]      = None,
                            reducer:  Option[ParallelDo] = None) extends MscrOutputChannel {

  lazy val tag = groupByKey.id
  def setup(implicit configuration: Configuration) { reducer.foreach(_.setup) }

  def createEmitter(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) = new EmitterWriter {
    def write(x: Any)  {
      sinks foreach { sink =>
        sink.configureCompression(configuration)
        channelOutput.write(tag, sink.id, sink.outputConverter.asInstanceOf[ToKeyValueConverter].asKeyValue(x))
      }
    }
  }

  def reduce(key: TaggedKey, untaggedValues: UntaggedValues, channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    val emitter: EmitterWriter = createEmitter(channelOutput)
    val values = combiner.map(c => c.combine(untaggedValues)).getOrElse(untaggedValues)
    reducer.map(_.reduce(key.get(tag), values, emitter)).getOrElse {
      combiner.map(c => emitter.write((key.get(tag), values))).getOrElse {
        emitter.write((key.get(tag), values))
      }
    }
  }

  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    val emitter = createEmitter(channelOutput)
    reducer.foreach(_.cleanup(emitter)(configuration))
  }

  override def toString =
    Seq(Some(groupByKey),
        combiner.map(n => "combiner = "+n.toString),
        reducer .map(n => "reducer  = "+n.toString)
    ).flatten.mkString("GbkOutputChannel(", ", ", ")")

  override def equals(a: Any) = a match {
    case o: GbkOutputChannel => o.groupByKey.id == groupByKey.id
    case _                   => false
  }
  override def hashCode = groupByKey.id.hashCode

  lazy val nodeSinks = groupByKey.sinks ++ combiner.toSeq.flatMap(_.sinks) ++ reducer.toSeq.flatMap(_.sinks)
  lazy val inputNodes = reducer.toSeq.map(_.env)
  lazy val bridgeStore = (reducer: Option[ProcessNode]).orElse(combiner).getOrElse(groupByKey).bridgeStore
}

case class BypassOutputChannel(output: ParallelDo) extends MscrOutputChannel {
  override def equals(a: Any) = a match {
    case o: BypassOutputChannel => o.output.id == output.id
    case _                      => false
  }
  override def hashCode = output.id.hashCode

  lazy val tag = output.id
  lazy val nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
  lazy val inputNodes = Seq(output.env)

  def setup(implicit configuration: Configuration) { output.setup }

  def createEmitter(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) = new EmitterWriter {
    def write(x: Any)  {
      sinks foreach { sink =>
        sink.configureCompression(configuration)
        channelOutput.write(tag, sink.id, sink.outputConverter.asInstanceOf[ToKeyValueConverter].asKeyValue(x))
      }
    }
  }

  def reduce(key: TaggedKey, untaggedValues: UntaggedValues, channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {
    implicit val sc = ScoobiConfigurationImpl(configuration)
    val emitter = createEmitter(channelOutput)
    untaggedValues foreach emitter.write
  }

  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) {}
}

object OutputChannel {
  implicit def outputChannelEqual = new Equal[OutputChannel] {
    def equal(a1: OutputChannel, a2: OutputChannel) = a1.tag == a2.tag
  }
}

object Channels extends control.ImplicitParameters {
  /** @return a sequence of distinct mapper input channels */
  def distinct(ins: Seq[MapperInputChannel]): Seq[MapperInputChannel] =
    ins.map(in => (in.sourceNode.id, in)).toMap.values.toSeq

  /** @return a sequence of distinct group by key output channels */
  def distinct(out: Seq[GbkOutputChannel])(implicit p: ImplicitParam): Seq[GbkOutputChannel] =
    out.map(o => (o.groupByKey.id, o)).toMap.values.toSeq

  /** @return a sequence of distinct bypass output channels */
  def distinct(out: Seq[BypassOutputChannel])(implicit p1: ImplicitParam1, p2: ImplicitParam2): Seq[BypassOutputChannel] =
    out.map(o => (o.output.id, o)).toMap.values.toSeq
}
