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
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import impl.io.Files


/** Class that abstracts all the common functionality of reading from text files. */
case class TextSource[A : WireFormat](paths: Seq[String],
                                      inputFormat: Class[_ <: FileInputFormat[LongWritable, Text]] = classOf[TextInputFormat],
                                      inputConverter: InputConverter[LongWritable, Text, A] = TextInput.defaultTextConverter,
                                      check: Source.InputCheck = Source.defaultInputCheck)
  extends DataSource[LongWritable, Text, A] {

  private val inputPaths = paths.map(p => new Path(p))
  override def toString = "TextSource("+id+")"+inputPaths.mkString("\n", "\n", "\n")

  def inputCheck(implicit sc: ScoobiConfiguration) { check(inputPaths, sc) }

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long =
    inputPaths.map(p => Files.pathSize(p)(sc.configuration)).sum
}

