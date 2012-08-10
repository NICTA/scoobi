package com.nicta.scoobi
package impl
package plan
package graph

import exec.UniqueId
import comp._

/**
 * This class represents an MSCR job with a Seq of input channels and a Seq of output channels
 * It is a mutable class which is constructed incrementally by using Kiama grammar attributes
 * @see MscrGraph
 */
case class Mscr(var inputChannels: Set[InputChannel] = Set(), var outputChannels: Set[OutputChannel] = Set()) {
  val id: Int = UniqueId.get

  override def toString =
    Seq(id.toString,
        if (inputChannels.isEmpty) "" else inputChannels.mkString("inputs: ", "\n", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("outputs: ",  "\n", "")).filterNot(_.isEmpty).mkString("Mscr(", "\n", ")")

  /** @return true if this mscr has no input or output channels */
  def isEmpty = inputChannels.isEmpty && outputChannels.isEmpty
  /** @return all the channels containing mappers, i.e. parallel dos */
  def mapperChannels = inputChannels.collect { case c @ MapperInputChannel(_) => c }
  /** @return all the id channels */
  def idChannels = inputChannels.collect { case c @ IdInputChannel(_) => c }
  /** @return all the flatten channels */
  def flattenChannels = outputChannels.collect { case c @ FlattenOutputChannel(_) => c }
  /** @return all the bypass output channels */
  def bypassChannels = outputChannels.collect { case c @ BypassOutputChannel(_) => c }
  /** @return all the input parallel dos of this mscr */
  def mappers        = mapperChannels.flatMap(_.parDos)
  /** @return an input channel containing a specific parallelDo */
  def inputChannelFor(m: ParallelDo[_,_,_]) = mapperChannels.find(_.parDos.contains(m))

  /** @return all the GroupByKeys of this mscr */
  def groupByKeys = outputChannels.collect { case GbkOutputChannel(gbk,_,_,_)            => gbk }
  /** @return all the reducers of this mscr */
  def reducers    = outputChannels.collect { case GbkOutputChannel(_,_,_,Some(reducer))  => reducer }
  /** @return all the combiners of this mscr */
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,_,Some(combiner),_) => combiner }
  /** @return all the parallelDos of this mscr */
  def parallelDos = mappers ++ reducers

  /** simultaneously set the input and output channels of this mscr */
  def addChannels(in: Set[InputChannel], out: Set[OutputChannel]) = {
   inputChannels  = (inputChannels ++ in)
   outputChannels = (outputChannels ++ out)
   this
  }
}

object Mscr {
  /** create an Mscr for related "floating" parallelDos */
  def floatingParallelDosMscr(pd: ParallelDo[_,_,_], siblings: Set[ParallelDo[_,_,_]]) = {
    Mscr(inputChannels  = Set(MapperInputChannel(siblings + pd)),
         outputChannels = (siblings + pd).map(BypassOutputChannel(_)))
  }
  /** create an Mscr for a "floating" Flatten */
  def floatingFlattenMscr(f: Flatten[_]) = {
    Mscr(outputChannels = Set(FlattenOutputChannel(f)),
         inputChannels  = f.ins.map {
                              case pd: ParallelDo[_,_,_] => MapperInputChannel(Set(pd))
                              case other                 => StraightInputChannel(other)
                          }.toSet)
  }
  /** @return a new empty Mscr */
  def empty = Mscr()
  /** @return true if a Mscr is created for GroupByKeys */
  def isGbkMscr: Mscr => Boolean = (_:Mscr).groupByKeys.nonEmpty
  /** @return true if a Mscr is created for floating parallel dos */
  def isParallelDoMscr: Mscr => Boolean = (_:Mscr).bypassChannels.nonEmpty
  /** @return true if a Mscr is created for floating flattens */
  def isFlattenMscr: Mscr => Boolean = (_:Mscr).flattenChannels.nonEmpty

}

