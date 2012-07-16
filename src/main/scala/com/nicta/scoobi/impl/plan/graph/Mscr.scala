package com.nicta.scoobi
package impl
package plan
package graph

import exec.UniqueId
import comp._

case class Mscr(var inputChannels: Seq[InputChannel] = Seq(), var outputChannels: Seq[OutputChannel] = Seq()) {
  val id: Int = UniqueId.get

  override def toString =
    Seq(id.toString,
        if (inputChannels.isEmpty) "" else inputChannels.mkString("inputs: ", ", ", ""),
        if (outputChannels.isEmpty) "" else outputChannels.mkString("outputs: ",  ",", "")).filterNot(_.isEmpty).mkString("Mscr(", ", ", ")")

  "Mscr ("+id+")"+inputChannels.mkString("\n(", ",", ")")+outputChannels.mkString("\n(", ",", ")")

  def mapperChannels = inputChannels.collect { case c @ MapperInputChannel(_) => c }
  def mappers        = mapperChannels.flatMap(_.parDos)
  def inputChannelFor(m: ParallelDo[_,_,_]) = mapperChannels.find(_.parDos.contains(m))

  def groupByKeys = outputChannels.collect { case GbkOutputChannel(gbk,_,_,_)            => gbk }
  def reducers    = outputChannels.collect { case GbkOutputChannel(_,_,_,Some(reducer))  => reducer }
  def combiners   = outputChannels.collect { case GbkOutputChannel(_,_,Some(combiner),_) => combiner }

  def addParallelDoOnMapperInputChannel(p: ParallelDo[_,_,_]) = {
    val (found, ic) = inputChannels.foldLeft((false, Seq[InputChannel]())) { (res, cur) =>
      val (f, channels) = res
      cur match {
        case MapperInputChannel(pdos) => (true, channels :+ MapperInputChannel(pdos :+ p))
        case other                    => (f,    channels :+ other)
      }
    }
    inputChannels = if (found) ic else (ic :+ MapperInputChannel(Seq(p)))
    this
  }

  def addGbkOutputChannel(gbk: GroupByKey[_,_]) = {
    outputChannels = outputChannels :+ GbkOutputChannel(gbk)
    this
  }

  def addGbkOutputChannel(gbk: GroupByKey[_,_], flatten: Flatten[_]) ={
    outputChannels = outputChannels :+ GbkOutputChannel(gbk, Some(flatten))
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

