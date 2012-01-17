/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.exec

import java.io.Serializable
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.Emitter


/** A producer of a TaggedMapper. */
trait MapperLike[A, K, V] {
  def mkTaggedMapper(tags: Set[Int]): TaggedMapper[A, K, V]
}


/** A wrapper for a 'map' function tagged for a specific output channel. */
abstract class TaggedMapper[A, K, V]
    (val tags: Set[Int])
    (implicit val mA: Manifest[A], val wtA: WireFormat[A],
              val mK: Manifest[K], val wtK: WireFormat[K], val ordK: Ordering[K],
              val mV: Manifest[V], val wtV: WireFormat[V])
  extends Serializable {

  /** The actual 'map' function that will be used by Hadoop in the mapper task. */
  def setup(): Unit
  def map(input: A, emitter: Emitter[(K, V)]): Unit
  def cleanup(emitter: Emitter[(K, V)]): Unit
}


/** A TaggedMapper that is an identity mapper. */
class TaggedIdentityMapper[K, V]
    (tags: Set[Int])
    (implicit mK: Manifest[K], wtK: WireFormat[K], ordK: Ordering[K],
              mV: Manifest[V], wtV: WireFormat[V],
              mKV: Manifest[(K, V)], wtKV: WireFormat[(K, V)])
  extends TaggedMapper[(K, V), K, V](tags)(mKV, wtKV, mK, wtK, ordK, mV, wtV) {

  /** Identity mapping */
  def setup() = {}
  def map(input: (K, V), emitter: Emitter[(K, V)]) = emitter.emit(input)
  def cleanup(emitter: Emitter[(K, V)]) = {}
}
