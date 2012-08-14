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
package exec

import core._

/** A producer of a TaggedReducer. */
trait ReducerLike[K, V, B, E] {
  def mkTaggedReducer(tag: Int): TaggedReducer[K, V, B, E]
}


/** A wrapper for a 'reduce' function tagged for a specific output channel. */
abstract case class TaggedReducer[K, V, B, E]
    (tag: Int)
    (implicit mK: Manifest[K], wtK: WireFormat[K], grpK: Grouping[K],
              mV: Manifest[V], wtV: WireFormat[V],
              val mB: Manifest[B], val wtB: WireFormat[B],
              mE: Manifest[E], wtE: WireFormat[E]) {

  /** The actual 'reduce' function that will be by Hadoop in the reducer task. */
  def setup(env: E)
  def reduce(env: E, key: K, values: Iterable[V], emitter: Emitter[B])
  def cleanup(env: E, emitter: Emitter[B])
}

/** A TaggedReducer that is an identity reducer. */
class TaggedIdentityReducer[B : Manifest : WireFormat](tag: Int)
  extends TaggedReducer[Int, B, B, Unit](tag)(implicitly[Manifest[Int]], implicitly[WireFormat[Int]], implicitly[Grouping[Int]],
                                              implicitly[Manifest[B]], implicitly[WireFormat[B]],
                                              implicitly[Manifest[B]], implicitly[WireFormat[B]],
                                              implicitly[Manifest[Unit]], implicitly[WireFormat[Unit]]) {

  /** Identity reducing - ignore the key. */
  def setup(env: Unit) {}
  def reduce(env: Unit, key: Int, values: Iterable[B], emitter: Emitter[B]) { values.foreach { emitter.emit(_) } }
  def cleanup(env: Unit, emitter: Emitter[B]) {}
}
