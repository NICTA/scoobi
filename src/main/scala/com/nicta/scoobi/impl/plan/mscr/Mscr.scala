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

import org.kiama.attribution.Attributable
import core._
import mscr.Mscr.ids
import scala.Tuple1
import scala.Some

/**
 * This class represents an MSCR job with a Seq of input channels and a Seq of output channels
 *
 * Each mscr has a unique id, which is used for equality.
 *
 * A Mscr has both input and output channels where input channels are doing mapping operations while output channels
 * are doing grouping and reducing operations.
 *
 * In order to optimise the processing a same value can be computed by several mapping functions and be directed to different
 * grouping / reducing functions, each of them identified by a tag.
 */
case class Mscr private (inputChannels: Seq[InputChannel] = Seq(), outputChannels: Seq[OutputChannel] = Seq()) extends Attributable {
  val id: Int = ids.get
  override def equals(a: Any) = a match {
    case m: Mscr => id == m.id
    case _       => false
  }
  override def hashCode = id.hashCode

  /** datasources for this Mscr */
  lazy val sources = inputChannels.map(_.source).distinct
  /** datasinks for this Mscr */
  lazy val sinks = outputChannels.flatMap(_.sinks).distinct
  /** bridges (source+sink) for this Mscr */
  lazy val bridges    = sinks.collect { case bs: Bridge => bs }
  /** distinct input nodes  for this Mscr */
  lazy val inputNodes = (inputChannels.flatMap(_.inputNodes) ++ outputChannels.flatMap(_.inputNodes)).distinct

  /** map of wireformats and groupings for each key tag of this Mscr */
  lazy val keyTypes   = KeyTypes(Map(inputChannels.flatMap(_.keyTypes.types):_*))
  /** map of wireformats for each input tag of this Mscr */
  lazy val valueTypes = ValueTypes(Map(inputChannels.flatMap(_.valueTypes.types):_*))

  override def toString =
    Seq(id.toString,
        if (inputChannels.isEmpty)  "" else inputChannels.mkString("\n  inputs: ", "\n          ", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("\n  outputs: ",  "\n           ", "")).filterNot(_.isEmpty).mkString("Mscr(", "\n", ")\n")

  /** @return all the combiners of this mscr */
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,Some(combiner),_) => combiner }
  /** @return all the combiners of this mscr by tag */
  def combinersByTag  = Map(outputChannels.collect { case out @ GbkOutputChannel(_,Some(combiner),_) => (out.tag, combiner) }: _*)

}

/**
 * Utility functions to create Mscrs
 */
object Mscr {
  object ids extends UniqueInt

  /** @return a Mscr from a single input and a single output */
  def create(input: InputChannel, output: OutputChannel): Mscr = create(Seq(input), Seq(output))
  def create(input: InputChannel, output: Seq[OutputChannel]): Mscr = create(Seq(input), output)
  def create(input: Seq[InputChannel], output: OutputChannel): Mscr = create(input, Seq(output))
  def create(input: Seq[InputChannel], output: Seq[OutputChannel]): Mscr = Mscr(input.distinct, output.distinct)
  def empty = create(Seq(), Seq())
}

/** encapsulation of expected key types for each tag */
case class KeyTypes(types: Map[Int, (WireReaderWriter, KeyGrouping)] = Map()) {
  def tags = types.keys.toSeq
  def add(tag: Int, wf: WireReaderWriter, gp: KeyGrouping) =
    copy(types = types + (tag -> (wf, gp)))
}

/** encapsulation of expected value types for each tag */
case class ValueTypes(types: Map[Int, Tuple1[WireReaderWriter]] = Map()) {
  def tags = types.keys.toSeq
  def add(tag: Int, wf: WireReaderWriter) =
    copy(types = types + (tag -> Tuple1(wf)))
}



