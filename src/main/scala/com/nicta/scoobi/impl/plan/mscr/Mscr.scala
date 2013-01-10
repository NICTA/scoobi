package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable

import util.UniqueId
import comp._
import core.{Bridge, Grouping, WireFormat}
import scalaz.Scalaz._

/**
 * This class represents an MSCR job with a Seq of input channels and a Seq of output channels
 * @see MscrGraph
 */
case class Mscr private (inputChannels: Seq[InputChannel] = Seq(), outputChannels: Seq[OutputChannel] = Seq()) extends Attributable {
  val id: Int = UniqueId.get

  lazy val keyTypes   = KeyTypes(Map(inputChannels.flatMap(_.keyTypes.types):_*))
  lazy val valueTypes = ValueTypes(Map(inputChannels.flatMap(_.valueTypes.types):_*))
  lazy val sources    = inputChannels.map(_.source).distinct
  lazy val sinks      = outputChannels.flatMap(_.sinks).distinct
  lazy val bridgeStores = sinks.collect { case bs: Bridge => bs }
  lazy val inputNodes = (inputChannels.flatMap(_.inputNodes) ++ outputChannels.flatMap(_.inputNodes)).distinct

  override def toString =
    Seq(id.toString,
        if (inputChannels.isEmpty) "" else inputChannels.mkString("  inputs: ", "\n          ", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("  outputs: ",  "\n           ", "")).filterNot(_.isEmpty).mkString("Mscr(", "\n", ")\n")

  /** @return all the combiners of this mscr */
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,Some(combiner),_) => combiner }
  /** @return all the combiners of this mscr by tag */
  def combinersByTag  = Map(outputChannels.collect { case out @ GbkOutputChannel(_,Some(combiner),_) => (out.tag, combiner) }: _*)

}

object Mscr {
  /** @return a Mscr from a single input and a single output */
  def create(input: InputChannel, output: OutputChannel): Mscr = create(Seq(input), Seq(output))
  def create(input: InputChannel, output: Seq[OutputChannel]): Mscr = create(Seq(input), output)
  def create(input: Seq[InputChannel], output: OutputChannel): Mscr = create(input, Seq(output))
  def create(input: Seq[InputChannel], output: Seq[OutputChannel]): Mscr = Mscr(input.distinct, output.distinct)
  def empty = create(Seq(), Seq())
}

case class KeyTypes(types: Map[Int, (WireFormat[_], Grouping[_])] = Map()) {
  def tags = types.keys.toSeq
  def add(tag: Int, wf: WireFormat[_], gp: Grouping[_]) =
    copy(types = types + (tag -> (wf, gp)))
}
case class ValueTypes(types: Map[Int, Tuple1[WireFormat[_]]] = Map()) {
  def tags = types.keys.toSeq
  def add(tag: Int, wf: WireFormat[_]) =
    copy(types = types + (tag -> Tuple1(wf)))
}



