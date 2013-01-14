package com.nicta.scoobi
package core

import org.apache.hadoop.conf.Configuration

/**
 * An object holder which can hold a distributed value
 */
trait Environment {
  /** push a value so that it's available to distributed processes */
  def push(env: Any)(implicit configuration: Configuration)
  /** get a distributed value */
  def pull(implicit configuration: Configuration): Any

}