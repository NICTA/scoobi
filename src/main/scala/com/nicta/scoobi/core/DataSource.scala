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
import scala.collection.JavaConversions._
import com.nicta.scoobi.impl.io.Helper
import java.io.IOException
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import impl.control.Exceptions._
import impl.util.Compatibility

/**
 * DataSource for a computation graph.
 *
 * It reads key-values (K, V) from the file system and uses an input converter to create a type A of input
 */
trait DataSource[K, V, A] extends Source {
  val id = ids.get
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

object Source {
  private lazy val logger = LogFactory.getLog("scoobi.Source")

  private[scoobi]
  def read(source: Source, read: Any => Any = identity)(implicit sc: ScoobiConfiguration): Seq[Any] = {
    val vb = new VectorBuilder[Any]()
    val job = new Job(new Configuration(sc.configuration))
    val inputFormat = source.inputFormat.newInstance

    job.setInputFormatClass(source.inputFormat)
    source.inputConfigure(job)

    val splits = tryOr(inputFormat.getSplits(job)) { case e: Throwable => logger.warn(e.getMessage); Seq() }
    splits foreach { split =>
      val tid = new TaskAttemptID()
      val taskContext = Compatibility.newTaskAttemptContext(job.getConfiguration, tid)
      val rr = inputFormat.createRecordReader(split, taskContext).asInstanceOf[RecordReader[Any, Any]]
      val mapContext = new InputOutputContext(Compatibility.newMapContext(job.getConfiguration, tid, rr, null, null, null, split))

      rr.initialize(split, taskContext)

      source.read(rr, mapContext, (a: Any) => vb += read(a))
      rr.close()
    }
    vb.result
  }

  /** default check for sources using input files */
  val defaultInputCheck = (inputPaths: Seq[Path], sc: ScoobiConfiguration) => {
    inputPaths foreach { p =>
      if (Helper.pathExists(p)(sc.configuration)) logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)(sc.configuration)) + ")")
      else                                        throw new IOException("Input path " + p + " does not exist.")
    }
  }
  val noInputCheck = (inputPaths: Seq[Path], sc: ScoobiConfiguration) => ()

  type InputCheck =  (Seq[Path], ScoobiConfiguration) => Unit
}

/**
 * Internal untyped version of an input converter
 */
private[scoobi]
trait FromKeyValueConverter {
  def asValue(context: InputOutputContext, key: Any, value: Any): Any
}

class InputOutputContext(val context: TaskInputOutputContext[Any,Any,Any,Any]) {
  def configuration = context.getConfiguration
  def write(key: Any, value: Any) { context.write(key, value) }
  def incrementCounter(groupName: String, name: String, increment: Long = 1L) {
    Option(context.getCounter(groupName, name)).foreach(_.increment(increment))
  }
  def getCounter(groupName: String, name: String) = {
    Option(context.getCounter(groupName, name).getValue).getOrElse(-1L)
  }
  def tick { context.progress() }
}