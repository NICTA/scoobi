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
package core

import org.apache.hadoop.mapreduce._
import Data._
import collection.immutable.VectorBuilder
import org.apache.hadoop.conf.Configuration
import task.{MapContextImpl, TaskAttemptContextImpl}
import scala.collection.JavaConversions._

/**
 * DataSource for a computation graph.
 *
 * It reads key-values (K, V) from the file system and uses an input converter to create a type A of input
 */
trait DataSource[K, V, A] extends Source {
  lazy val id = ids.get
  def inputFormat: Class[_ <: InputFormat[K, V]]
  def inputCheck(implicit sc: ScoobiConfiguration)
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration)
  def inputSize(implicit sc: ScoobiConfiguration): Long
  def inputConverter: InputConverter[K, V, A]
  def fromKeyValueConverter = inputConverter

  private[scoobi]
  def read(reader: RecordReader[_,_], mapContext: InputOutputContext, read: Any => Unit) {
    while (reader.nextKeyValue) {
      read(fromKeyValueConverter.asValue(mapContext, reader.getCurrentKey, reader.getCurrentValue))
    }
  }
}
/**
 * Convert an InputFormat's key-value types to the type produced by a source
 */
trait InputConverter[K, V, A] extends FromKeyValueConverter {
  type InputContext = MapContext[K, V, _, _]
  def asValue(context: InputOutputContext, key: Any, value: Any): Any = fromKeyValue(context.context.asInstanceOf[InputContext], key.asInstanceOf[K], value.asInstanceOf[V])
  def fromKeyValue(context: InputContext, key: K, value: V): A
}

/**
 * Internal untyped version of a DataSource
 */
private[scoobi]
trait Source {
  /** @return a unique id for this source */
  def id: Int
  /** the InputFormat specifying the type of input for this source */
  def inputFormat: Class[_ <: InputFormat[_,_]]
  /** check the validity of the source specification */
  def inputCheck(implicit sc: ScoobiConfiguration)
  /** configure the source. */
  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration)
  /** @return the size in bytes of the data being input by this source */
  def inputSize(implicit sc: ScoobiConfiguration): Long
  /** map the key-values of a source's InputFormat to the final type produced by it */
  def fromKeyValueConverter: FromKeyValueConverter

  private[scoobi]
  def read(reader: RecordReader[_,_], mapContext: InputOutputContext, read: Any => Unit)
}

private[scoobi]
object Source {
  def read(source: Source, read: Any => Any)(implicit sc: ScoobiConfiguration): Seq[Any] = {
    val vb = new VectorBuilder[Any]()
    val job = new Job(new Configuration(sc.configuration))
    val inputFormat = source.inputFormat.newInstance

    job.setInputFormatClass(source.inputFormat)
    source.inputConfigure(job)

    inputFormat.getSplits(job) foreach { split =>
      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rr = inputFormat.createRecordReader(split, taskContext).asInstanceOf[RecordReader[Any, Any]]
      val mapContext = InputOutputContext(new MapContextImpl(job.getConfiguration, tid, rr, null, null, null, split))

      rr.initialize(split, taskContext)

      source.read(rr, mapContext, (a: Any) => vb += read(a))
      rr.close()
    }
    vb.result
  }
}

/**
 * Internal untyped version of an input converter
 */
private[scoobi]
trait FromKeyValueConverter {
  def asValue(context: InputOutputContext, key: Any, value: Any): Any
}

case class InputOutputContext(context: MapContext[Any,Any,Any,Any]) {
  def configuration = context.getConfiguration
  def write(key: Any, value: Any) { context.write(key, value) }
}