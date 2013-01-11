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
import org.apache.hadoop.io.compress.CompressionCodec

trait OutputChannel extends Channel {
  lazy val id: Int = UniqueId.get

  override def equals(a: Any) = a match {
    case o: OutputChannel => o.id == id
    case other            => false
  }
  override def hashCode = id.hashCode

  def sinks: Seq[Sink]
  def inputNodes: Seq[CompNode]

  def tag: Int

  def setup(implicit configuration: Configuration)
  def reduce(key: TaggedKey, untaggedValues: UntaggedValues, channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)
  def cleanup(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration)
  def collectOutputs(outputFiles: Seq[Path])(implicit configuration: ScoobiConfiguration, fileSystems: FileSystems) {
    val fs = configuration.fileSystem
    import fileSystems._

    sinks.zipWithIndex.foreach { case (sink, i) =>
      sink.outputPath foreach { outDir =>
        fs.mkdirs(outDir)
        val files = outputFiles filter isResultFile(tag, i)
        files foreach moveTo(outDir)
      }
    }
  }

}

trait MscrOutputChannel extends OutputChannel {
  def sinks = bridgeStore +: nodeSinks
  protected def nodeSinks: Seq[Sink]
  def bridgeStore: Bridge
}

case class GbkOutputChannel(groupByKey:   GroupByKey,
                            combiner: Option[Combine]      = None,
                            reducer:  Option[ParallelDo] = None) extends MscrOutputChannel {

  def tag = groupByKey.id
  def setup(implicit configuration: Configuration) { reducer.foreach(_.setup) }

  def createEmitter(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) = new EmitterWriter {
    def write(x: Any)  {
      sinks.zipWithIndex foreach { case (sink, i) =>
        sink.configureCompression(configuration)
        channelOutput.write(tag, i, sink.outputConverter.asInstanceOf[ToKeyValueConverter].asKeyValue(x))
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

  def tag = output.id
  lazy val nodeSinks = output.sinks
  lazy val bridgeStore = output.bridgeStore
  lazy val inputNodes = Seq(output.env)

  def setup(implicit configuration: Configuration) { output.setup }

  def createEmitter(channelOutput: ChannelOutputFormat)(implicit configuration: Configuration) = new EmitterWriter {
    def write(x: Any)  {
      sinks.zipWithIndex foreach { case (sink, i) =>
        sink.configureCompression(configuration)
        channelOutput.write(tag, i, sink.outputConverter.asInstanceOf[ToKeyValueConverter].asKeyValue(x))
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
    def equal(a1: OutputChannel, a2: OutputChannel) = a1.id == a2.id
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
