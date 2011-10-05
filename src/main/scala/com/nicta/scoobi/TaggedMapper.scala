/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io.Serializable


/** A prodcuer of a TaggedMapper. */
trait MapperLike[A, K, V] {
  def mkTaggedMapper(tag: Int): TaggedMapper[A, K, V]
  def mkIdentityMapper(tag: Int): TaggedIdentityMapper[K,V]
}


/** A wrapper for a 'map' function tagged for a specific output channel. */
abstract class TaggedMapper[A, K, V]
    (val tag: Int)
    (implicit val mA: Manifest[A], val wtA: HadoopWritable[A],
              val mK: Manifest[K], val wtK: HadoopWritable[K], val ordK: Ordering[K],
              val mV: Manifest[V], val wtV: HadoopWritable[V])
  extends Serializable {

  /** The acutal 'map' function that will be by Hadoop in the mapper task. */
  def map(value: A): Iterable[(K, V)]
}


/** A TaggedMapper that is an identity mapper. */
class TaggedIdentityMapper[K, V]
    (tag: Int)
    (implicit mK: Manifest[K], wtK: HadoopWritable[K], ordK: Ordering[K],
              mV: Manifest[V], wtV: HadoopWritable[V],
              mKV: Manifest[(K, V)], wtKV: HadoopWritable[(K, V)])
  extends TaggedMapper[(K, V), K, V](tag)(mKV, wtKV, mK, wtK, ordK, mV, wtV) {

  /** Identity mapping */
  def map(value: (K, V)): Iterable[(K, V)] = List(value)
}
