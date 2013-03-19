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
package testing

import core._
import application._
import org.specs2.specification.Fragments
import org.specs2.specification.Tags

/**
 * This trait can be used to create Hadoop specifications on the NictaCluster
 */
abstract class NictaHadoop extends
  HadoopSpecification with
  Tags with
  NictaCluster {

  /**this type alias makes it shorter to pass a new configuration object to each example */
  type SC = ScoobiConfiguration

  def acceptanceSection = section("hadoop")

  override def map(fs: =>Fragments) = super.map(fs).insert(acceptanceSection).add(acceptanceSection)
}

/**
 * Addresses for the filesystem and jobtracker for the Nicta cluster. They override the search for those values in the local configuration files
 */
trait NictaCluster extends Cluster {
  override def fs         = "hdfs://svm-hadoop1.ssrg.nicta.com.au"
  override def jobTracker = "svm-hadoop1.ssrg.nicta.com.au:8021"
}

/**
 * A trait for simple jobs running on the NICTA cluster
 */
abstract class NictaSimpleJobs extends NictaHadoop with SimpleJobs