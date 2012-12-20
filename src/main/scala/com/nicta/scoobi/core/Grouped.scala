package com.nicta.scoobi
package core

import impl.collection.Iterable1

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
  def mapValues[W](f: Iterable1[V] => Iterable1[W])(implicit MK: Manifest[K], FK: WireFormat[K], MW: Manifest[W], FW: WireFormat[W]): Grouped[K, W] =
    Grouped(list map (_ mapValues f))

  /**
   * Run a function on each value in the distributed list to produce a distributed list with new values. Synonym for `:->`.
   */
  def map[W](f: V => W)(implicit MK: Manifest[K], FK: WireFormat[K], MW: Manifest[W], FW: WireFormat[W]): Grouped[K, W] =
    Grouped(list map (_ map f))

  /**
   * Run a function on each value in the distributed list to produce a distributed list with new values. Synonym for `map`.
   */
  def :->[W](f: V => W)(implicit MK: Manifest[K], FK: WireFormat[K], MW: Manifest[W], FW: WireFormat[W]): Grouped[K, W] =
    map(f)

  /**
   * Run a function on each key in the distributed list to produce a distributed list with new key. Synonym for `<-:`.
   */
  def mapKeys[L](f: K => L)(implicit ML: Manifest[L], FL: WireFormat[L], MV: Manifest[V], FV: WireFormat[V]): Grouped[L, V] =
    Grouped(list map (_ mapKey f))

  /**
   * Run a function on each key in the distributed list to produce a distributed list with new key. Synonym for `mapKeys`.
   */
  def <-:[L](f: K => L)(implicit ML: Manifest[L], FL: WireFormat[L], MV: Manifest[V], FV: WireFormat[V]): Grouped[L, V] =
    mapKeys(f)

  /**
   * The keys of the underlying distributed list.
   */
  def keys(implicit MK: Manifest[K], FK: WireFormat[K]): DList[K] =
    list map (_.key)

  /**
   * The values of the underlying distributed list grouped by their key.
   */
  def values(implicit MW: Manifest[V], FW: WireFormat[V]): DList[Iterable1[V]] =
    list map (_.values)

  /**
   * The values of the underlying distributed list flattened.
   */
  def valuesF(implicit MW: Manifest[V], FW: WireFormat[V]): DList[V] =
    list flatMap (_.values.toIterable)

  /**
   * Map two functions on the keys and values (binary map) of the distributed list.
   */
  def bimap[L, W](f: K => L, g: V => W)(implicit ML: Manifest[L], FL: WireFormat[L], MW: Manifest[W], FW: WireFormat[W]): Grouped[L, W] =
    Grouped(list map (_ bimap (f, g)))

}

object Grouped {
  /**
   * Construct a `Grouped` with the given distributed list.
   */
  def apply[K, V](x: DList[Association1[K, V]]): Grouped[K, V] =
    new Grouped[K, V] {
      val list =
        x
    }
}
