/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io.Serializable


/** A prodcuer of a TaggedMapper. */
trait MapperLike[A, K, V] {
  def mkTaggedMapper(tags: Set[Int]): TaggedMapper[A, K, V]

}

trait KVLike[K,V] {
  def mkTaggedIdentityMapper(tags: Set[Int]): TaggedIdentityMapper[K,V]
}


/** A wrapper for a 'map' function tagged for a specific output channel. */
abstract class TaggedMapper[A, K, V]
    (val tags: Set[Int])
    (implicit val mA: Manifest[A], val wtA: HadoopWritable[A],
              val mK: Manifest[K], val wtK: HadoopWritable[K], val ordK: Ordering[K],
              val mV: Manifest[V], val wtV: HadoopWritable[V])
  extends Serializable {

  /** The actual 'map' function that will be by Hadoop in the mapper task. */
  def map(value: A): Iterable[(K, V)]
}


/** A TaggedMapper that is an identity mapper. */
class TaggedIdentityMapper[K, V]
    (tags: Set[Int])
    (implicit mK: Manifest[K], wtK: HadoopWritable[K], ordK: Ordering[K],
              mV: Manifest[V], wtV: HadoopWritable[V],
              mKV: Manifest[(K, V)], wtKV: HadoopWritable[(K, V)])
  extends TaggedMapper[(K, V), K, V](tags)(mKV, wtKV, mK, wtK, ordK, mV, wtV) {

  /** Identity mapping */
  def map(value: (K, V)): Iterable[(K, V)] = List(value)
}
