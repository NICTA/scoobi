package com.nicta.scoobi.testing

import com.nicta.scoobi.ScoobiConfiguration
import mutable.SimpleJobs
import org.specs2.specification.Fragments
import org.specs2.mutable.Tags

/**
 * This trait can be used to create Hadoop specifications on the NictaCluster
 */
trait NictaHadoop extends
  mutable.HadoopSpecification with
  NictaTags                   with
  NictaCluster {

  /** this type alias makes it shorter to pass a new configuration object to each example */
  type SC = ScoobiConfiguration

  override def map(fs: =>Fragments) = super.map(fs).insert(acceptanceSection).add(acceptanceSection)
}

/**
 * a mutable specification for the Nicta cluster
 */
trait NictaHadoopSpecification extends NictaHadoop with org.specs2.mutable.Specification

/**
 * examples running on the cluster will be tagged as "acceptance"
 */
trait NictaTags extends Tags { this: NictaHadoop =>
  // all the examples will be tagged as "acceptance" since they are using the local hadoop installation
  // or the cluster
  def acceptanceSection = section("acceptance", "local", "cluster")
}

/**
 * Addresses for the filesystem and jobtracker for the Nicta cluster. They override the search for those values in the local configuration files
 */
trait NictaCluster extends HadoopHomeDefinedCluster {
  override def fs         = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
  override def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
}

/**
 * A trait for simple jobs running on the NICTA cluster
 */
trait NictaSimpleJobs extends NictaHadoop with SimpleJobs

