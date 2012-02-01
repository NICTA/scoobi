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
package com.nicta.scoobi.io.seq

import java.io.IOException
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.rtt.ScoobiWritable


/** Smart functions for materializing distributed lists by loading Sequence files. */
object SequenceInput {
  lazy val logger = LogFactory.getLog("scoobi.SequenceInput")

  /** Create a new DList from the contents of one or more Sequence Files. Note that the type parameters K and V
    * must match the type key-value type of the Sequence Files. In the case of a directory being specified,
    * the input forms all the files in that directory. */
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: String*): DList[(K, V)] =
    fromSequenceFile(List(paths: _*))

  /** Create a new DList from the contents of a list of one or more Sequence Files. Note
    * that the type parameters K and V must match the type key-value type of the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in
    * that directory. */
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: List[String]): DList[(K, V)] = {
    val loader = new Loader[(K, V)] {
      def mkInputStore(node: AST.Load[(K, V)]) = new InputStore[K, V, (K, V)](node) {
        private val inputPaths = paths.map(p => new Path(p))

        val inputFormat = classOf[SequenceFileInputFormat[K, V]]

        def inputCheck() = inputPaths foreach { p =>
          if (Helper.pathExists(p))
            logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)) + ")")
          else
             throw new IOException("Input path" + p + " does not exist.")
        }

        def inputConfigure(job: Job) = inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }

        def inputSize(): Long = inputPaths.map(p => Helper.pathSize(p)).sum

        val inputConverter = new InputConverter[K, V, (K, V)] {
          def fromKeyValue(k: K, v: V) = (k, v)
        }
      }
    }
    new DList(Smart.Load(loader))
  }
}
