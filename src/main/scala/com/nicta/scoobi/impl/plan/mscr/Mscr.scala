package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable

import util.UniqueId
import comp._
import collection._
import core.{Grouping, WireFormat, CompNode}
import scalaz.Scalaz._

/**
 * This class represents an MSCR job with a Seq of input channels and a Seq of output channels
 * It is a mutable class which is constructed incrementally by using Kiama grammar attributes
 * @see MscrGraph
 */
case class Mscr(inputChannels: Seq[InputChannel] = Seq(), outputChannels: Seq[OutputChannel] = Seq()) extends Attributable {
  val id: Int = UniqueId.get

  lazy val keyTypes   = KeyTypes(inputChannels.map(_.keyTypes).foldMap)
  lazy val valueTypes = ValueTypes(inputChannels.map(_.valueTypes).foldMap)
  lazy val sources    = inputChannels.flatMap(_.sources).distinct
  lazy val sinks      = outputChannels.flatMap(_.sinks).distinct


  def channels = inputChannels ++ outputChannels
  /** @return the nodes which are incomings to this Mscr */
  def incomings = inputChannels.flatMap(_.incomings) ++ outputChannels.flatMap(_.environment)

  /** it is necessary to override the generated equality method in order to use the vars */
  override def equals(a: Any) = a match {
    case m: Mscr => inputChannels == m.inputChannels && outputChannels == m.outputChannels
    case _       => false
  }
  override def toString =
    Seq(id.toString,
        if (inputChannels.isEmpty) "" else inputChannels.mkString("  inputs: ", "\n          ", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("  outputs: ",  "\n           ", "")).filterNot(_.isEmpty).mkString("Mscr(", "\n", ")\n")

  /** @return true if this mscr has no input or output channels */
  def isEmpty = inputChannels.isEmpty && outputChannels.isEmpty
  /** @return all the channels containing mappers, i.e. parallel dos */
  def mapperChannels = inputChannels.collect { case c : MapperInputChannel => c }
  /** @return all the id channels */
  def idChannels = inputChannels.collect { case c: IdInputChannel => c }
  /** @return all the gbk output channels */
  def gbkOutputChannels = outputChannels.collect { case c: GbkOutputChannel => c }
  /** @return all the bypass output channels */
  def bypassOutputChannels = outputChannels.collect { case c: BypassOutputChannel => c }
  /** @return all the input parallel dos of this mscr */
  def mappers = mapperChannels.flatMap(_.parDos)
  /** @return all the input parallel dos of this mscr in id channels */
  def idMappers = idChannels.collect { case IdInputChannel(p : ParallelDo[_,_,_],_,_,_) => p }
  /** @return an input channel containing a specific parallelDo */
  def inputChannelFor(m: ParallelDo[_,_,_]) = mapperChannels.find(_.parDos.contains(m))

  /** @return all the GroupByKeys of this mscr */
  def groupByKeys = outputChannels.collect { case GbkOutputChannel(gbk,_,_,_)            => gbk }
  /** @return all the reducers of this mscr */
  def reducers    = outputChannels.collect { case GbkOutputChannel(_,_,Some(reducer),_)  => reducer }
  /** @return all the combiners of this mscr */
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,Some(combiner),_,_) => combiner }
  /** @return all the combiners of this mscr by tag */
  def combinersByTag  = Map(outputChannels.collect { case out @ GbkOutputChannel(_,Some(combiner),_,_) => (out.tag, combiner) }: _*)
  /** @return all the parallelDos of this mscr */
  def parallelDos = mappers ++ reducers

  /** simultaneously set the input and output channels of this mscr */
  def addChannels(in: Seq[InputChannel], out: Seq[OutputChannel]) =
    copy(inputChannels  = (inputChannels ++ in), outputChannels = (outputChannels ++ out))

  /** @return true if the node is contained in one of the input channels */
  def inputContains(node: CompNode) =
    inputChannels.exists(_.contains(node))

  /** @return true if the node is contained in one of the output channels */
  def outputContains(node: CompNode) =
    outputChannels.exists(_.contains(node))

  /** @return true if the node is contained in one of the input or output channels */
  def contains(node: CompNode) = inputContains(node) || outputContains(node)
}

object Mscr {
  /** @return a Mscr from a single input and a single output */
  def apply(input: InputChannel, output: OutputChannel): Mscr = Mscr(Seq(input), Seq(output))
  def apply(input: InputChannel, output: Seq[OutputChannel]): Mscr = Mscr(Seq(input), output)
  def apply(input: Seq[InputChannel], output: OutputChannel): Mscr = Mscr(input, Seq(output))

  /** create an Mscr for related "floating" parallelDos */
  def floatingParallelDosMscr(pd: ParallelDo[_,_,_], siblings: Seq[ParallelDo[_,_,_]]) = {
    Mscr(outputChannels = (siblings :+ pd).map(BypassOutputChannel(_)))
  }
  /** @return a new empty Mscr */
  def empty = Mscr()
  /** @return true if a Mscr is created for GroupByKeys */
  def isGbkMscr: Mscr => Boolean = (_:Mscr).groupByKeys.nonEmpty
  /** @return true if a Mscr is created for floating parallel dos */
  def isParallelDoMscr: Mscr => Boolean = (_:Mscr).bypassOutputChannels.nonEmpty

}

case class KeyTypes(types: Map[Int, (WireFormat[_], Grouping[_])] = Map()) {
  def add(tag: Int, wf: WireFormat[_], gp: Grouping[_]) =
    copy(types = types + (tag -> (wf, gp)))
}
case class ValueTypes(types: Map[Int, Tuple1[WireFormat[_]]] = Map()) {
  def add(tag: Int, wf: WireFormat[_]) =
    copy(types = types + (tag -> Tuple1(wf)))
}



