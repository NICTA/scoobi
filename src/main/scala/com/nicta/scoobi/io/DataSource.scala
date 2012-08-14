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

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.MapContext
import org.apache.hadoop.mapreduce.Job

import application.ScoobiConfiguration


/** An input data store to a MapReduce job. */
trait DataSource[K, V, A] {
  /** The InputFormat specifying the type of input for this DataSource. */
  def inputFormat: Class[_ <: InputFormat[K, V]]

  /** Check the validity of the DataSource specification. */
  def inputCheck(sc: ScoobiConfiguration)

  /** Configure the DataSource. */
  def inputConfigure(job: Job)

  /** Size in bytes of the data being input by this source. */
  def inputSize: Long

  /** Maps the key-values of a DataSource's InputFormat to the final type produced by it. */
  def inputConverter: InputConverter[K, V, A]
}


/** Convert an InputFormat's key-value types to the type produced by a DataSource. */
trait InputConverter[K, V, A] {
  type InputContext = MapContext[K, V, _, _]
  def fromKeyValue(context: InputContext, key: K, value: V): A
}
