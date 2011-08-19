/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._


/** A wrapper for a 'reducing' function. */
class Reducer[K2 : HadoopWritable : Ordering,
              V2 : HadoopWritable,
              K3 : HadoopWritable : Ordering,
              V3 : HadoopWritable](
    f: (K2, List[V2]) => List[(K3, V3)],
    val k3WritableComparableClass: Class[_],
    val v3WritableClass: Class[_])
  extends Serializable {

  def reduce(key: K2, values: List[V2]): List[(K3, V3)] = f(key, values)
}

