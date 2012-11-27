package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attributable

import util.UniqueId
import comp._
import collection._
import IdSet._
import core.CompNode

/**
 * This class represents an MSCR job with a Seq of input channels and a Seq of output channels
 * It is a mutable class which is constructed incrementally by using Kiama grammar attributes
 * @see MscrGraph
 */
case class Mscr(var inputChannels: Set[InputChannel] = Set(), var outputChannels: Set[OutputChannel] = Set()) extends Attributable {
  val id: Int = UniqueId.get

  /** @return the nodes which are incomings to this Mscr */
  def incomings = inputChannels.toSeq.flatMap(_.incomings) ++ outputChannels.toSeq.flatMap(_.environment)

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
  /** @return all the flatten channels */
  def flattenOutputChannels = outputChannels.collect { case c: FlattenOutputChannel => c }
  /** @return all the gbk output channels */
  def gbkOutputChannels = outputChannels.collect { case c: GbkOutputChannel => c }
  /** @return all the bypass output channels */
  def bypassOutputChannels = outputChannels.collect { case c: BypassOutputChannel => c }
  /** @return all the input parallel dos of this mscr */
  def mappers = mapperChannels.flatMap(_.parDos)
  /** @return all the input parallel dos of this mscr in id channels */
  def idMappers = idChannels.collect { case IdInputChannel(p : ParallelDo[_,_,_],_) => p }
  /** @return an input channel containing a specific parallelDo */
  def inputChannelFor(m: ParallelDo[_,_,_]) = mapperChannels.find(_.parDos.contains(m))

  /** @return all the GroupByKeys of this mscr */
  def groupByKeys = outputChannels.collect { case GbkOutputChannel(gbk,_,_,_,_)            => gbk }
  /** @return all the reducers of this mscr */
  def reducers    = outputChannels.collect { case GbkOutputChannel(_,_,_,Some(reducer),_)  => reducer }
  /** @return all the combiners of this mscr */
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,_,Some(combiner),_,_) => combiner }
  /** @return all the parallelDos of this mscr */
  def parallelDos = mappers ++ reducers

  /** simultaneously set the input and output channels of this mscr */
  def addChannels(in: Set[InputChannel], out: Set[OutputChannel]) = {
   inputChannels  = (inputChannels ++ in)
   outputChannels = (outputChannels ++ out)
   this
  }

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
  def apply(input: InputChannel, output: OutputChannel): Mscr = Mscr(IdSet(input), Set(output))
  def apply(input: InputChannel, output: Seq[OutputChannel]): Mscr = Mscr(Set(input), output.toSet)
  def apply(input: Seq[InputChannel], output: OutputChannel): Mscr = Mscr(input.toSet, Set(output))

  /** create an Mscr for related "floating" parallelDos */
  def floatingParallelDosMscr(pd: ParallelDo[_,_,_], siblings: Set[ParallelDo[_,_,_]]) = {
    Mscr(inputChannels  = IdSet(MapperInputChannel(siblings + pd)),
         outputChannels = (siblings + pd).map(BypassOutputChannel(_)))
  }
  /** create an Mscr for a "floating" Flatten */
  def floatingFlattenMscr(f: Flatten[_]) = {
    Mscr(outputChannels = IdSet(FlattenOutputChannel(f)),
         inputChannels  = f.ins.map {
                              case pd: ParallelDo[_,_,_] => MapperInputChannel(pd)
                              case other                 => StraightInputChannel(other)
                          }.toIdSet.asInstanceOf[Set[InputChannel]])
  }
  /** @return a new empty Mscr */
  def empty = Mscr()
  /** @return true if a Mscr is created for GroupByKeys */
  def isGbkMscr: Mscr => Boolean = (_:Mscr).groupByKeys.nonEmpty
  /** @return true if a Mscr is created for floating parallel dos */
  def isParallelDoMscr: Mscr => Boolean = (_:Mscr).bypassOutputChannels.nonEmpty
  /** @return true if a Mscr is created for floating flattens */
  def isFlattenMscr: Mscr => Boolean = (_:Mscr).flattenOutputChannels.nonEmpty

}

