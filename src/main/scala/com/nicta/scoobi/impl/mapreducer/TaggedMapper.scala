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

import core._
import util.UniqueInt

/** A wrapper for a 'map' function tagged for a specific output channel. */
abstract class TaggedMapper(val tags: Set[Int], mwfk: ManifestWireFormat[_], val gpk: Grouping[_], mwfv: ManifestWireFormat[_]) {

  def mfk = mwfk.mf
  def wfk = mwfk.wf
  def mfv = mwfv.mf
  def wfv = mwfv.wf

  object RollingInt extends UniqueInt

  /** setup(env: E) */
  def setup(env: Any)
  /** map(env: E, input: A, emitter: Emitter[(K, V)]) */
  def map(env: Any, input: Any, emitter: Emitter[Any])
  /** cleanup(env: E, emitter: Emitter[(K, V)]) */
  def cleanup(env: Any, emitter: Emitter[Any])
}


/** A TaggedMapper that is an identity mapper. */
class TaggedIdentityMapper(tags: Set[Int], mwfk: ManifestWireFormat[_], gpk: Grouping[_], mwfv: ManifestWireFormat[_]) extends TaggedMapper(tags, mwfk, gpk, mwfv) {

  /** setup(env: Unit) */
  def setup(env: Any) {}
  /** map(env: Unit, input: A, emitter: Emitter[(K, V)]) */
  def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit(input) }
  /** cleanup(env: Unit, emitter: Emitter[(K, V)]) */
  def cleanup(env: Any, emitter: Emitter[Any]) {}
}
