/**
  * Copyright 2011 National ICT Australia Limited
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


/* An output store from a MapReduce job. */
trait DataSink[K, V, B] {
  /** The OutputFormat specifying the type of output for this DataSink. */
  def outputFormat: Class[_ <: OutputFormat[K, V]]

  /** The Class of the OutputFormat's key. */
  def outputKeyClass: Class[K]

  /** The Class of the OutputFormat's value. */
  def outputValueClass: Class[V]

  /** Check the validity of the DataSink specification. */
  def outputCheck(sc: ScoobiConfiguration)

  /** Configure the DataSink. */
  def outputConfigure(job: Job)

  /** Maps the type consumed by this DataSink to the key-values of its OutputFormat. */
  def outputConverter: OutputConverter[K, V, B]
}


/** Convert the type consumed by a DataSink into an OutputFormat's key-value types. */
trait OutputConverter[K, V, B] {
  def toKeyValue(x: B): (K, V)
}
