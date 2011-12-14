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
import com.nicta.scoobi.Grouping
import com.nicta.scoobi.Emitter


/** A producer of a TaggedReducer. */
trait ReducerLike[K, V, B] {
  def mkTaggedReducer(tag: Int): TaggedReducer[K, V, B]
}


/** A wrapper for a 'reduce' function tagged for a specific output channel. */
abstract class TaggedReducer[K, V, B]
    (val tag: Int)
    (implicit val mK: Manifest[K], val wtK: WireFormat[K], val grpK: Grouping[K],
              val mV: Manifest[V], val wtV: WireFormat[V],
              val mB: Manifest[B], val wtB: WireFormat[B])
  extends Serializable {

  /** The actual 'reduce' function that will be by Hadoop in the reducer task. */
  def reduce(key: K, values: Iterable[V], emitter: Emitter[B]): Unit
}

/** A TaggedReducer that is an identity reducer. */
class TaggedIdentityReducer[B]
    (tag: Int)
    (implicit mK: Manifest[Int], wtK: WireFormat[Int], grpK: Grouping[Int],
              mB: Manifest[B], wtB: WireFormat[B])
  extends TaggedReducer[Int, B, B](tag)(mK, wtK, grpK, mB, wtB, mB, wtB) {

  /** Identity reducing - ignore the key. */
  def reduce(key: Int, values: Iterable[B], emitter: Emitter[B]) = values.foreach { emitter.emit(_) }
}
