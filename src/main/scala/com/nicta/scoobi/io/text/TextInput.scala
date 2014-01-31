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

import java.io.{DataInput, IOException}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import core._
import impl.plan.DListImpl
import impl.mapreducer.TaggedInputSplit
import impl.ScoobiConfiguration._
import impl.io.Files
import WireFormat._
import com.nicta.scoobi.application.InputsOutputs

/**
 * Smart functions for materialising distributed lists by loading text files.
 */
trait TextInput {

  /**
   * Create a DList from one or more files or directories (in the case of a directory, the input forms all files in that directory).
   */
  def fromTextFile(paths: String*): DList[String] =
    fromTextSource[String](textSource(paths))

  def fromTextFiles(paths: Seq[String], check: Source.InputCheck = Source.defaultInputCheck): DList[String] =
    fromTextSource[String](textSource(paths, check))

  /**
   * Create a DList[(String, String)] from one or more files or directories where
   *   key   = the path name of the read file
   *   value = a line in the file
   */
  def fromPartitionedTextFiles(paths: String*): DList[(String, String)] =
  fromTextSource[(String, String)](partitionedTextSource(paths))

  /** Create a distributed list from a list of one ore more files or directories (in the case of
    * a directory, the input forms all files in that directory). The file(s) contain a number
    * of fields delimited by a separator. Use an extractor function to pull out the required
    * fields to create the distributed list. */
  def fromDelimitedTextFiles[A : WireFormat](paths: Seq[String], sep: String = "\t", check: Source.InputCheck = Source.defaultInputCheck)
                                            (extractFn: PartialFunction[Seq[String], A]): DList[A] = {
    val lines = fromTextSource(textSource(paths, check))
    lines.mapFlatten { line =>
      val fields = line.split(sep).toList
      if (extractFn.isDefinedAt(fields)) List(extractFn(fields)) else Nil
    }
  }

  /** Create a distributed list from a text file that is a number of fields delimited
    * by some separator. Use an extractor function to pull out the required fields to
    * create the distributed list. */
  def fromDelimitedTextFile[A : WireFormat]
  (path: String, sep: String = "\t", check: Source.InputCheck = Source.defaultInputCheck)
  (extractFn: PartialFunction[Seq[String], A]): DList[A] =
    fromDelimitedTextFiles(Seq(path), sep, check)(extractFn)

  /**
   * TEXT SOURCES
   */
  def fromTextSource[A : WireFormat](source: DataSource[_,_,A]) =
    InputsOutputs.fromSource(source)

  /** create a text source */
  def textSource(paths: Seq[String], check: Source.InputCheck = Source.defaultInputCheck) =
    new TextSource[String](paths, inputConverter = defaultTextConverter, check = check)

  /** create a text source for partitioned files */
  def partitionedTextSource(paths: Seq[String], check: Source.InputCheck = Source.defaultInputCheck) =
    new PartitionedTextSource[(String, String)](paths, inputConverter = defaultTextConverterWithPath, check = check)

  /**
   * INPUT CONVERTERS
   */
  def defaultTextConverter = new InputConverter[LongWritable, Text, String] {
    def fromKeyValue(context: InputContext, k: LongWritable, v: Text) = v.toString
  }

  def defaultTextConverterWithPath = new InputConverter[Text, Text, (String, String)] {
    def fromKeyValue(context: InputContext, k: Text, v: Text) = (k.toString, v.toString)
  }

  /**
   * EXTRACTORS
   */
  private type NFE = java.lang.NumberFormatException

  /** Extract an Int from a String. */
  object AnInt {
    def unapply(s: String): Option[Int] =
      try { Some(s.toInt) } catch { case _: NFE => None }
  }

  /** Extract a Long from a String. */
  object ALong {
    def unapply(s: String): Option[Long] =
      try { Some(s.toLong) } catch { case _: NFE => None }
  }

  /** Extract a Double from a String. */
  object ADouble {
    def unapply(s: String): Option[Double] =
      try { Some(s.toDouble) } catch { case _: NFE => None }
  }

  /** Extract a Float from a String. */
  object AFloat {
    def unapply(s: String): Option[Float] =
      try { Some(s.toFloat ) } catch { case _: NFE => None }
  }

}

object TextInput extends TextInput

