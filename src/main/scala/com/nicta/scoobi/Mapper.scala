/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._


/** A wrapper for a 'mapping' function. */
class Mapper[K1 : HadoopWritable : Ordering,
             V1 : HadoopWritable,
             K2 : HadoopWritable : Ordering,
             V2 : HadoopWritable](
    f: (K1, V1) => List[(K2, V2)],
    val k2WritableComparableClass: Class[_],
    val v2WritableClass: Class[_])
  extends Serializable {

  def map(key: K1, value: V1): List[(K2, V2)] = f(key, value)

}

