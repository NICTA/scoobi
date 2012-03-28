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
package com.nicta.scoobi.io.avro

import java.io.IOException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat

import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.AST


/** Smart functions for materializing distributed lists by loading Avro files. */
object AvroInput {
  lazy val logger = LogFactory.getLog("scoobi.AvroInput")


  /** Create a new DList from the contents of one or more Avro files. The type of the DList must conform to
    * the schema types allowed by Avro, as constrained by the 'AvroSchema' type class. In the case of a directory
    * being specified, the input forms all the files in that directory. */
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: String*): DList[A] = fromAvroFile(List(paths: _*))


  /** Create a new DList from the contents of a list of one or more Avro files. The type of the
    * DList must conform to the schema types allowed by Avro, as constrained by the 'AvroSchema' type
    * class. In the case of a directory being specified, the input forms all the files in
    * that directory. */
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: List[String]): DList[A] = {
    val sch = implicitly[AvroSchema[A]]

    val loader = new Loader[A] {
      def mkInputStore(node: AST.Load[A]) = new InputStore[AvroKey[sch.AvroType], NullWritable, A](node) {
        private val inputPaths = paths.map(p => new Path(p))

        val inputFormat = classOf[AvroKeyInputFormat[sch.AvroType]]

        def inputCheck() = inputPaths foreach { p =>
          if (Helper.pathExists(p)) {
            logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)) + ")")
            logger.debug("Input schema: " + sch.schema)
          } else {
             throw new IOException("Input path " + p + " does not exist.")
          }
        }

        def inputConfigure(job: Job) = {
          inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
          job.getConfiguration.set("avro.schema.input.key", sch.schema.toString)
        }

        def inputSize(): Long = inputPaths.map(p => Helper.pathSize(p)).sum

        val inputConverter = new InputConverter[AvroKey[sch.AvroType], NullWritable, A] {
          def fromKeyValue(context: InputContext, k: AvroKey[sch.AvroType], v: NullWritable) = sch.fromAvro(k.datum)
        }
      }
    }

    new DList(Smart.Load(loader))
  }
}
