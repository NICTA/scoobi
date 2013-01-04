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

import application._
import lib._
import core._

/** Global Scoobi functions and values. */
object Scoobi extends core.WireFormatImplicits with core.GroupingImplicits with Application with InputsOutputs with Persist with Library with DObjects {

  /* Primary types */
  type WireFormat[A] = com.nicta.scoobi.core.WireFormat[A]
  val DList = DLists
  type DList[A] = com.nicta.scoobi.core.DList[A]
  implicit def traversableToDList[A : Manifest : WireFormat](trav: Traversable[A]) = DList.traversableToDList(trav)

  val DObject = DObjects
  type DObject[A] = com.nicta.scoobi.core.DObject[A]

  type DoFn[A, B] = com.nicta.scoobi.core.DoFn[A, B]
  type BasicDoFn[A, B] = com.nicta.scoobi.core.BasicDoFn[A, B]
  type EnvDoFn[A, B, E] = com.nicta.scoobi.core.EnvDoFn[A, B, E]

  val Grouping = com.nicta.scoobi.core.Grouping
  type Grouping[A] = com.nicta.scoobi.core.Grouping[A]

  type Emitter[A] = com.nicta.scoobi.core.Emitter[A]
}



