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

