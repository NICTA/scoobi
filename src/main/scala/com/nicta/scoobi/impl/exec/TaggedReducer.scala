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

/** A wrapper for a 'reduce' function tagged for a specific output channel. */
abstract class TaggedReducer(val tag: Int, val mfb: Manifest[_], val wfb: WireFormat[_]) {
  /** The actual 'reduce' function that will be by Hadoop in the reducer task. */
  def setup(env: Any)
  def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any])
  def cleanup(env: Any, emitter: Emitter[Any])
}

/** A TaggedReducer that is an identity reducer. */
class TaggedIdentityReducer(tag: Int, mb: Manifest[_], wfb: WireFormat[_]) extends TaggedReducer(tag, mb, wfb) {
  def setup(env: Any) {}
  def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any])  { values.foreach { emitter.emit(_) } }
  def cleanup(env: Any, emitter: Emitter[Any]) {}
}

