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

import java.io.IOException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import impl.exec.TaggedInputSplit
import core._
import application.ScoobiConfiguration


/** Smart functions for materializing distributed lists by loading text files. */
object TextInput {
  lazy val logger = LogFactory.getLog("scoobi.TextInput")

  /** Create a distributed list from one or more files or directories (in the case of a directory,
    * the input forms all files in that directory). */
  def fromTextFile(paths: String*): DList[String] = fromTextFile(List(paths: _*))


  /** Create a distributed list from a list of one or more files or directories (in the case of
    * a directory, the input forms all files in that directory). */
  def fromTextFile(paths: List[String]): DList[String] = {
    val converter = new InputConverter[LongWritable, Text, String] {
      def fromKeyValue(context: InputContext, k: LongWritable, v: Text) = v.toString
    }

    DList.fromSource(new TextSource(paths, converter))
  }


  /** Create a distributed list from one or more files or directories (in the case of
    * a directory, the input forms all files in that directory). The distributed list is a tuple
    * where the first part is the path of the originating file and the second part is a line of
    * text. */
  def fromTextFileWithPath(paths: String*): DList[(String, String)] = fromTextFileWithPath(paths: _*)


  /** Create a distributed list from a list of one or more files or directories (in the case of
    * a directory, the input forms all files in that directory). The distributed list is a tuple
    * where the first part is the path of the originating file and the second part is a line of
    * text. */
  def fromTextFileWithPath(paths: List[String]): DList[(String, String)] = {
    val converter = new InputConverter[LongWritable, Text, (String, String)] {
      def fromKeyValue(context: InputContext, k: LongWritable, v: Text) = {
        val taggedSplit = context.getInputSplit.asInstanceOf[TaggedInputSplit]
        val fileSplit = taggedSplit.inputSplit.asInstanceOf[FileSplit]
        val path = fileSplit.getPath.toUri.toASCIIString
        (path, v.toString)
      }
    }

    DList.fromSource(new TextSource(paths, converter))
  }


  /** Create a distributed list from a text file that is a number of fields delimited
    * by some separator. Use an extractor function to pull out the required fields to
    * create the distributed list. */
  def fromDelimitedTextFile[A : Manifest : WireFormat]
      (path: String, sep: String = "\t")
      (extractFn: PartialFunction[List[String], A])
    : DList[A] = {

    val lines = fromTextFile(path)
    lines.flatMap { line =>
      val fields = line.split(sep).toList
      if (extractFn.isDefinedAt(fields)) List(extractFn(fields)) else Nil
    }
  }


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


  /* Class that abstracts all the common functionality of reading from text files. */
  class TextSource[A : Manifest : WireFormat](paths: List[String], converter: InputConverter[LongWritable, Text, A])
    extends DataSource[LongWritable, Text, A] {

    // default config until its set through inputConfigure or outputConfigure
    var config: Configuration = new Configuration

    private val inputPaths = paths.map(p => new Path(p))

    val inputFormat = classOf[TextInputFormat]

    def inputCheck(sc: ScoobiConfiguration) {
      inputPaths foreach { p =>
        if (Helper.pathExists(p)(sc))
          logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)(sc)) + ")")
        else
          throw new IOException("Input path " + p + " does not exist.")
      }
    }

    def inputConfigure(job: Job) = {
      config = job.getConfiguration
      inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
    }

    def inputSize: Long = inputPaths.map(p => Helper.pathSize(p)(config)).sum

    lazy val inputConverter = converter
  }
}
