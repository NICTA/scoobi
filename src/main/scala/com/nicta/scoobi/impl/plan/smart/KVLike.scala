package com.nicta.scoobi
package impl
package plan
package smart

import exec.TaggedIdentityMapper


trait KVLike[K,V] {
  def mkTaggedIdentityMapper(tags: Set[Int]): TaggedIdentityMapper[K,V]
}
