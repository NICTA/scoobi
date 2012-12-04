package com.nicta.scoobi
package testing
package mutable

import org.specs2.mutable.Tags
import core.ScoobiConfiguration
import org.specs2.specification.Fragments

/**
 * This trait can be used to create Hadoop specifications on the NictaCluster
 */
trait NictaHadoop extends
  mutable.HadoopSpecification with
  Tags with
  NictaCluster {

  /**this type alias makes it shorter to pass a new configuration object to each example */
  type SC = ScoobiConfiguration

  def acceptanceSection = section("hadoop")

  override def map(fs: =>Fragments) = super.map(fs).insert(acceptanceSection).add(acceptanceSection)
}

/**
 * A trait for simple jobs running on the NICTA cluster
 */
trait NictaSimpleJobs extends NictaHadoop with SimpleJobs