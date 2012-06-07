package com.nicta.scoobi
package impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This trait can be used on DataSource/DataSink traits in order to store the configuration when using the "inputConfigure/outputConfigure" methods.
 * Then, the configuration can be used
 */
trait Configured {
  protected implicit var conf: Configuration = new Configuration

  protected def configure(job: Job) {
    conf = job.getConfiguration
    checkPaths
  }

  /**
   * path checking is only done once the configuration is set.
   * Otherwise, if this code is executed inside the 'inputCheck/outputCheck' methods, the configuration object would not be
   * referencing the adequate remote file system
   */
  protected def checkPaths()
}
