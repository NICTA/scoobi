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

/* A wrapper around an object that is part of the graph of a distributed computation.*/
trait DObject[A] {
  implicit def mwf: ManifestWireFormat[A]
  implicit def mf = mwf.mf
  implicit def wf = mwf.wf

  private[scoobi]
  def getComp: CompNode

  /**Create a new distributed object by apply a function to this distributed object. */
  def map[B : ManifestWireFormat](f: A => B): DObject[B]

  /**Create a new distributed list by replicating the value of this distributed object
   * to every element within the provided distributed list. */
  def join[B : ManifestWireFormat](list: DList[B]): DList[(A, B)]
  def join[B : ManifestWireFormat](o: DObject[B]): DObject[(A, B)]

  def toSingleElementDList: DList[A]
  def toDList[B](implicit ev: A <:< Iterable[B], mwfb: ManifestWireFormat[B]): DList[B] = toSingleElementDList.flatMap(x => x)
}


