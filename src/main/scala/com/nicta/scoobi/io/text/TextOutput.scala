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
package text

import core._

/** Smart functions for persisting distributed lists by storing them as text files. */
trait TextOutput {

  /** Persist a distributed lists of 'Products' (e.g. Tuples) as a delimited text file. */
  def listToDelimitedTextFile[A <: Product : Manifest](dl: DList[A], path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) = {
    (dl map { anyToString(_, sep) }).addSink(textFileSink[A](path, overwrite, check))
  }

  /** Persist a distributed object of 'Products' (e.g. Tuples) as a delimited text file. */
  def objectToDelimitedTextFile[A <: Product : Manifest](o: DObject[A], path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) = {
    (o map { anyToString(_, sep) }).addSink(textFileSink[A](path, overwrite, check))
  }

  def anyToString(any: Any, sep: String): String = any match {
    case list: List[_] => list.map(anyToString(_, sep)).mkString(sep)
    case prod: Product => prod.productIterator.map(anyToString(_, sep)).mkString(sep)
    case _             => any.toString
  }

  /**
   * SINKS
   */
  def textFileSink[A : Manifest](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    new TextFileSink(path, overwrite, check)

  def textFilePartitionedSink[K : Manifest, V : Manifest](path: String,
                                                          partition: K => String,
                                                          overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    new TextFilePartitionedSink(path, partition, overwrite, check)(implicitly[Manifest[K]], implicitly[Manifest[V]])
}

object TextOutput extends TextOutput
