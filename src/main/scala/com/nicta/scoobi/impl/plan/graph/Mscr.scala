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
        if (inputChannels.isEmpty) "" else inputChannels.mkString("inputs: ", ", ", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("outputs: ",  ",", "")).filterNot(_.isEmpty).mkString("Mscr(", ", ", ")")

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

  def addParallelDoOnMapperInputChannel(p: ParallelDo[_,_,_]) = {
    val (found, ic) = inputChannels.foldLeft((false, Set[InputChannel]())) { (res, cur) =>
      val (f, channels) = res
      cur match {
        case MapperInputChannel(pdos) => (true, channels + MapperInputChannel(pdos + p))
        case other                    => (f,    channels + other)
      }
    }
    inputChannels = if (found) ic else (ic + MapperInputChannel(Set(p)))
    this
  }

  def addChannels(in: Set[InputChannel], out: Set[OutputChannel]) = {
   inputChannels  = (inputChannels ++ in)
   outputChannels = (outputChannels ++ out)
   this
  }

  def addInputChannels(ins: Seq[CompNode]) = {
    ins foreach {
      case pd: ParallelDo[_,_,_] => inputChannels += MapperInputChannel(Set(pd))
      case other                 => inputChannels += StraightInputChannel(other)
    }
    this
  }

  def addGbkOutputChannel(gbk: GroupByKey[_,_]) = {
    outputChannels = outputChannels + GbkOutputChannel(gbk)
    this
  }

  def addGbkOutputChannel(gbk: GroupByKey[_,_], flatten: Flatten[_]) ={
    outputChannels = outputChannels + GbkOutputChannel(gbk, Some(flatten))
    this
  }

  def addCombinerOnGbkOutputChannel(gbk: GroupByKey[_,_], cb: Combine[_,_]) =
    updateOutputChannel(gbk) { o: GbkOutputChannel => o.copy(combiner = Some(cb))}

  def addReducerOnGbkOutputChannel(gbk: GroupByKey[_,_], pd: ParallelDo[_,_,_]) =
    updateOutputChannel(gbk) { o: GbkOutputChannel =>  o.copy(reducer = Some(pd))}

  private def updateOutputChannel(gbk: GroupByKey[_,_])(f: GbkOutputChannel => GbkOutputChannel) = {
    outputChannels = outputChannels.map {
      case o @ GbkOutputChannel(g,_,_,_) if g.id == gbk.id => f(o)
      case other                                           => other
    }
    this
  }

}

object Mscr {
  def parallelDosMscr(pd: ParallelDo[_,_,_], siblings: Set[ParallelDo[_,_,_]]) = {
    Mscr(inputChannels  = Set(MapperInputChannel(siblings + pd)),
         outputChannels = (siblings + pd).map(BypassOutputChannel(_)))
  }
  def empty = Mscr()
  /** @return true if a Mscr is created for GroupByKeys */
  def isGbkMscr: Mscr => Boolean = (_:Mscr).groupByKeys.nonEmpty
  /** @return true if a Mscr is created for floating parallel dos */
  def isParallelDoMscr: Mscr => Boolean = (_:Mscr).bypassChannels.nonEmpty
  /** @return true if a Mscr is created for floating flattens */
  def isFlattenMscr: Mscr => Boolean = (_:Mscr).flattenChannels.nonEmpty

}

