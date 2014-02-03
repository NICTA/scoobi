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
package com.nicta
package scoobi
package io
package text

import core._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import impl.io.Files

/**
 * Read files in different paths, specified as globs.
 *
 * For example if paths are created as dir/month=12/day=03 the glob should be dir/star/star (where star = *)
 *
 * Then the full path will be used as the key for each value in a given file
 */
case class PartitionedTextSource[A : WireFormat](paths: Seq[String],
                                                 inputFormat: Class[_ <: FileInputFormat[Text, Text]] = classOf[PathTextInputFormat],
                                                 inputConverter: InputConverter[Text, Text, A] = TextInput.defaultTextConverterWithPath,
                                                 check: Source.InputCheck = Source.defaultInputCheck)
  extends DataSource[Text, Text, A] {

  private val inputPaths = paths.map(p => new Path(p))

  override def toString = "PartitionedTextSource("+id+")"+inputPaths.mkString("\n", "\n", "\n")

  def inputCheck(implicit sc: ScoobiConfiguration) { check(inputPaths, sc) }

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long =
    inputPaths.map(p => Files.pathSize(p)(sc.configuration)).sum
}