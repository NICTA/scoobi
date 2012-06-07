package com.nicta.scoobi
package impl
package collection

import scala.collection._

/**
 * This trait provides utility methods on maps, especially mutable maps
 */
trait Maps {

  implicit def extendMutableMap[K, V](map: mutable.Map[K, V]) = new ExtendedMutableMap[K, V](map)
  class ExtendedMutableMap[K, V](map: mutable.Map[K, V]) {
    /**
     * update a mutable map with new keys and values existing in another map
     * @return the original mutable map
     */
    def updateWith(other: Map[K, V])(update: PartialFunction[(K, V), (K, V)]) = {
      (other -- map.keys) foreach { kv =>
        if (update.isDefinedAt(kv)) {
          map += update.apply(kv)
        }
      }
      map
    }
  }

}

object Maps extends Maps
