package com.nicta.scoobi.application

import org.apache.hadoop.fs.FileSystem

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

  def configuration: ScoobiConfiguration

  def fs = configuration.get(FileSystem.FS_DEFAULT_NAME_KEY, "file:///")
  def jobTracker = configuration.get("mapred.job.tracker", "local")
}

