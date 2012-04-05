package com.nicta.scoobi.testing

/**
 * Definition of the Cluster addresses
 */
trait Cluster {
  /** @return the filesystem address  */
  def fs: String
  /** @return the jobtracker address  */
  def jobTracker: String
}
