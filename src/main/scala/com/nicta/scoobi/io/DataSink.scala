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
package io

import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.Job

import application.ScoobiConfiguration
import org.apache.hadoop.io.compress.{CompressionCodec, SplittableCompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


/* An output store from a MapReduce job. */
trait DataSink[K, V, B] { outer =>
  /** The OutputFormat specifying the type of output for this DataSink. */
  def outputFormat: Class[_ <: OutputFormat[K, V]]

  /** The Class of the OutputFormat's key. */
  def outputKeyClass: Class[K]

  /** The Class of the OutputFormat's value. */
  def outputValueClass: Class[V]

  /** Check the validity of the DataSink specification. */
  def outputCheck(implicit sc: ScoobiConfiguration)

  /** Configure the DataSink. */
  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration)

  /** Maps the type consumed by this DataSink to the key-values of its OutputFormat. */
  def outputConverter: OutputConverter[K, V, B]

  /** Set the compression configuration */
  def outputCompression(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = new DataSink[K, V, B] {
    def outputFormat: Class[_ <: OutputFormat[K, V]]                = outer.outputFormat
    def outputKeyClass: Class[K]                                    = outer.outputKeyClass
    def outputValueClass: Class[V]                                  = outer.outputValueClass
    def outputCheck(implicit sc: ScoobiConfiguration)               = outer.outputCheck(sc)
    def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) = outer.outputConfigure(job)
    def outputConverter: OutputConverter[K, V, B]                   = outer.outputConverter
    /** configure the job so that the output is compressed */
    override def configureCompression(job: Job) = {
      job.getConfiguration.set("mapred.output.compress", "true")
      job.getConfiguration.set("mapred.output.compression.type", compressionType.toString())
      job.getConfiguration.setClass("mapred.output.compression.codec", codec.getClass, classOf[CompressionCodec])
      job
    }
  }

  /** configure the compression for a given job */
  def configureCompression(job: Job) = job

  /** @return the output path of this sink */
  def outputPath(implicit sc: ScoobiConfiguration) = {
    val jobCopy = new Job(sc)
    outputConfigure(jobCopy)
    Option(FileOutputFormat.getOutputPath(jobCopy))
  }
}


/** Convert the type consumed by a DataSink into an OutputFormat's key-value types. */
trait OutputConverter[K, V, B] {
  def toKeyValue(x: B): (K, V)
}
