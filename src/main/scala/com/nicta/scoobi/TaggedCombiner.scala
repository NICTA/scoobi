/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io.Serializable


/** A producer of a TaggedCombiner. */
trait CombinerLike[V] {
  def mkTaggedCombiner(tag: Int): TaggedCombiner[V]
}


/** A wrapper for a 'combine' function tagged for a specific output channel. */
abstract class TaggedCombiner[V]
    (val tag: Int)
    (implicit val mV: Manifest[V], val wtV: HadoopWritable[V])
  extends Serializable {

  /** The acutal 'combine' function that will be called by Hadoop at the
    * completion of the mapping phase. */
  def combine(x: V, y: V): V
}
