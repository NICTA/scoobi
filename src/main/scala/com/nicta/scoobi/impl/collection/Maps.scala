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
