package com.nicta.scoobi.impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

trait Configured {
  protected implicit var conf: Configuration = new Configuration

  protected def configure(job: Job) {
    conf = job.getConfiguration
  }
}
