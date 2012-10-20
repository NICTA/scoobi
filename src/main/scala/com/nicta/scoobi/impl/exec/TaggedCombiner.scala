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

/** A producer of a TaggedCombiner. */
trait CombinerLike[V] {
  def mkTaggedCombiner(tag: Int): TaggedCombiner[V]
}


/** A wrapper for a 'combine' function tagged for a specific output channel. */
abstract class TaggedCombiner[V]
    (val tag: Int)
    (implicit val mV: Manifest[V], val wtV: WireFormat[V]) {

  /** The actual 'combine' function that will be called by Hadoop at the
    * completion of the mapping phase. */
  def combine(x: V, y: V): V
}
