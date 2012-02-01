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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.DList
import com.nicta.scoobi.DListPersister
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.AST


/** Smart functions for persisting distributed lists by storing them as Sequence files. */
object SequenceOutput {
  lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  /** Specify a distributed list to be persistent by storing it to disk as a Sequence File. */
  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: DList[(K, V)], path: String): DListPersister[(K, V)] = {
    val persister = new Persister[(K, V)] {
      def mkOutputStore(node: AST.Node[(K, V)]) = new OutputStore[K, V, (K, V)](node) {
        private val outputPath = new Path(path)

        val outputFormat = classOf[SequenceFileOutputFormat[K, V]]
        val outputKeyClass = implicitly[Manifest[K]].erasure.asInstanceOf[Class[K]]
        val outputValueClass = implicitly[Manifest[V]].erasure.asInstanceOf[Class[V]]

        def outputCheck() =
          if (Helper.pathExists(outputPath))
            throw new FileAlreadyExistsException("Output path already exists: " + outputPath)
          else
            logger.info("Output path: " + outputPath.toUri.toASCIIString)

        def outputConfigure(job: Job) = FileOutputFormat.setOutputPath(job, outputPath)

        val outputConverter = new OutputConverter[K, V, (K, V)] {
          def toKeyValue(x: (K, V)) = (x._1, x._2)
        }
      }
    }
    new DListPersister(dl, persister)
  }
}
