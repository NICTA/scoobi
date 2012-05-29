package com.nicta.scoobi.testing.mutable

import org.specs2.mutable.Tags

/**
 * examples running on the cluster will be tagged as "acceptance"
 */
trait HadoopTags extends Tags { this: HadoopSpecification =>
  // all the examples will be tagged as "hadoop" since they are using the local hadoop installation
  // or the cluster
  def acceptanceSection = section("hadoop")
}
