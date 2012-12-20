package com.nicta.scoobi
package impl
package plan
package mscr

import core.{Sink, Source}
import mapreducer.{TaggedReducer, TaggedCombiner, TaggedMapper, Env}

trait MscrJob {
  type T <: MscrJob

  def addTaggedMapper(inputs: Seq[Source], env: Option[Env[_]], m: TaggedMapper): T
  def addTaggedCombiner[V](c: TaggedCombiner[_]): T
  def addTaggedReducer(outputs: scala.collection.Seq[Sink], env: Option[Env[_]], r: TaggedReducer): T
}
