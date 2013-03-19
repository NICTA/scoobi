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

/**
 * A distributed list of associations.
 *
 * @see [[com.nicta.scoobi.core.Association1]]
 */
sealed trait Grouped[K, V] {
  /**
   * The underlying distributed list.
   */
  val list: DList[Association1[K, V]]

  /**
   * Run a function on the values of the distributed list to produce new values.
   */
  def mapValues[W](f: Iterable1[V] => Iterable1[W])(implicit fk: WireFormat[K], fw: WireFormat[W]): Grouped[K, W] =
    Grouped(list map (_ mapValues f))

  /**
   * Run a function on each value in the distributed list to produce a distributed list with new values. Synonym for `:->`.
   */
  def map[W](f: V => W)(implicit fk: WireFormat[K], fw: WireFormat[W]): Grouped[K, W] =
    Grouped(list map (_ map f))

  /**
   * Run a function on each value in the distributed list to produce a distributed list with new values. Synonym for `map`.
   */
  def :->[W](f: V => W)(implicit fk: WireFormat[K], fw: WireFormat[W]): Grouped[K, W] =
    map(f)

  /**
   * Run a function on each key in the distributed list to produce a distributed list with new key. Synonym for `<-:`.
   */
  def mapKeys[L](f: K => L)(implicit fl: WireFormat[L], fv: WireFormat[V]): Grouped[L, V] =
    Grouped(list map (_ mapKey f))

  /**
   * Run a function on each key in the distributed list to produce a distributed list with new key. Synonym for `mapKeys`.
   */
  def <-:[L](f: K => L)(implicit fl: WireFormat[L], fv: WireFormat[V]): Grouped[L, V] =
    mapKeys(f)

  /**
   * The keys of the underlying distributed list.
   */
  def keys(implicit fk: WireFormat[K]): DList[K] =
    list map (_.key)

  /**
   * The values of the underlying distributed list grouped by their key.
   */
  def values(implicit fv: WireFormat[V]): DList[Iterable1[V]] =
    list map (_.values)

  /**
   * The values of the underlying distributed list flattened.
   */
  def valuesF(implicit fv: WireFormat[V]): DList[V] =
    list mapFlatten (_.values.toIterable)

  /**
   * Map two functions on the keys and values (binary map) of the distributed list.
   */
  def bimap[L, W](f: K => L, g: V => W)(implicit fl: WireFormat[L], fw: WireFormat[W]): Grouped[L, W] =
    Grouped(list map (_ bimap (f, g)))

}

object Grouped {
  /**
   * Construct a `Grouped` with the given distributed list.
   */
  def apply[K, V](x: DList[Association1[K, V]]): Grouped[K, V] =
    new Grouped[K, V] {
      val list = x
    }
}
