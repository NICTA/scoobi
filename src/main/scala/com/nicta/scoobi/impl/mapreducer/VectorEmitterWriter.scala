package com.nicta.scoobi
package impl
package mapreducer

import scala.collection.immutable.VectorBuilder
import core._
import plan.comp.ParallelDo


/**
 * In memory emitter writer saving the values to a Vector
 */
case class VectorEmitterWriter() extends EmitterWriter {
  private val vb = new VectorBuilder[Any]
  def write(v: Any) { vb += v }
  def result = vb.result

  /** use this emitter to map a list of value with a parallelDo */
  def map(environment: Any, mappedValues: Seq[Any], mapper: ParallelDo)(implicit configuration: ScoobiConfiguration) = {
    vb.clear
    mappedValues.foreach(v => mapper.map(environment, v, this))
    result
  }
}

