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
package com.nicta.scoobi.io.text

import java.io.IOException
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MapContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import com.nicta.scoobi.Scoobi
import com.nicta.scoobi.DList
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.io.InputConverter
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.exec.TaggedInputSplit


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

    new DList(Smart.Load(new TextLoader(paths, converter)))
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

    new DList(Smart.Load(new TextLoader(paths, converter)))
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
  object Int {
    def unapply(s: String): Option[Int] =
      try { Some(s.toInt) } catch { case _: NFE => None }
  }

  /** Extract a Long from a String. */
  object Long {
    def unapply(s: String): Option[Long] =
      try { Some(s.toLong) } catch { case _: NFE => None }
  }

  /** Extract a Double from a String. */
  object Double {
    def unapply(s: String): Option[Double] =
      try { Some(s.toDouble) } catch { case _: NFE => None }
  }

  /** Extract a Float from a String. */
  object Float {
    def unapply(s: String): Option[Float] =
      try { Some(s.toFloat ) } catch { case _: NFE => None }
  }


  /* Class that abstracts all the common functionality of reading from text files. */
  class TextLoader[A : Manifest : WireFormat](paths: List[String], converter: InputConverter[LongWritable, Text, A]) extends Loader[A] {
    def mkInputStore(node: AST.Load[A]) = new InputStore[LongWritable, Text, A](node) {
      private val inputPaths = paths.map(p => new Path(p))

      val inputFormat = classOf[TextInputFormat]

      def inputCheck() = inputPaths foreach { p =>
        if (Helper.pathExists(p))
          logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)) + ")")
        else
           throw new IOException("Input path " + p + " does not exist.")
      }

      def inputConfigure(job: Job) = inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }

      def inputSize(): Long = inputPaths.map(p => Helper.pathSize(p)).sum

      val inputConverter = converter
    }
  }
}
