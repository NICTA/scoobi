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

/** A producer of a TaggedMapper. */
trait MapperLike[A, E, K, V] {
  def mkTaggedMapper(tags: Set[Int]): TaggedMapper[A, E, K, V]
}


/** A wrapper for a 'map' function tagged for a specific output channel. */
abstract case class TaggedMapper[A, E, K, V]
    (tags: Set[Int])
    (implicit mA: Manifest[A], wtA: WireFormat[A],
              mE: Manifest[E], wtE: WireFormat[E],
              val mK: Manifest[K], val wtK: WireFormat[K], val grpK: Grouping[K],
              val mV: Manifest[V], val wtV: WireFormat[V]) {

  /** The actual 'map' function that will be used by Hadoop in the mapper task. */
  def setup(env: E)
  def map(env: E, input: A, emitter: Emitter[(K, V)])
  def cleanup(env: E, emitter: Emitter[(K, V)])
}


/** A TaggedMapper that is an identity mapper. */
class TaggedIdentityMapper[K, V]
    (tags: Set[Int])
    (implicit mK: Manifest[K], wtK: WireFormat[K], grpK: Grouping[K],
              mV: Manifest[V], wtV: WireFormat[V],
              mKV: Manifest[(K, V)], wtKV: WireFormat[(K, V)])
  extends TaggedMapper[(K, V), Unit, K, V](tags)(mKV, wtKV, implicitly[Manifest[Unit]], implicitly[WireFormat[Unit]], mK, wtK, grpK, mV, wtV) {

  /** Identity mapping */
  def setup(env: Unit) {}
  def map(env: Unit, input: (K, V), emitter: Emitter[(K, V)]) { emitter.emit(input) }
  def cleanup(env: Unit, emitter: Emitter[(K, V)]) {}
}
