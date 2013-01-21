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
package application

import org.apache.hadoop.fs.FileSystem._
import impl.ScoobiConfiguration
import ScoobiConfiguration._
/**
 * Definition of the Cluster addresses: FileSystem + JobTracker
 */
trait Cluster {
  /**@return the filesystem address  */
  def fs: String

  /**@return the jobtracker address  */
  def jobTracker: String
}


/**
 * Implementation of the Cluster trait taking the configuration from a ScoobiConfiguration object
 */
trait ClusterConfiguration extends Cluster {

  def configuration: com.nicta.scoobi.core.ScoobiConfiguration

  def fs         = configuration.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)
  def jobTracker = configuration.get("mapred.job.tracker", "local")
}

