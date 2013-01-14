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
package core

import impl.plan.comp.ValueNode

/**
 *  A wrapper around an object that is part of the graph of a distributed computation
 */
trait DObject[A] extends Persistent[A] {
  type C <: ValueNode

  implicit def wf: WireFormat[A]

  /** Create a new distributed object by apply a function to this distributed object */
  def map[B : WireFormat](f: A => B): DObject[B]

  /**
   * Create a new distributed list by replicating the value of this distributed object
   * to every element within the provided distributed list
   */
  def join[B : WireFormat](list: DList[B]): DList[(A, B)]
  def join[B : WireFormat](o: DObject[B]): DObject[(A, B)]

  def toSingleElementDList: DList[A]
  def toDList[B](implicit ev: A <:< Iterable[B], wfb: WireFormat[B]): DList[B] = toSingleElementDList.flatMap(x => x)
}


