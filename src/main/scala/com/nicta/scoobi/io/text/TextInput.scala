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
import java.io.Serializable
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

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


/** Smart functions for materializing distributed lists by loading text files. */
object TextInput {
  lazy val logger = LogFactory.getLog("scoobi.TextInput")

  /** Create a distributed list from one or more files or directories (in the case of a directory,
    * the input forms all files in that directory). */
  def fromTextFile(paths: String*): DList[String] = fromTextFile(List(paths: _*))

  /** Create a distributed list from a list of one or more files or directories (in the case of
    * a directory, the input forms all files in that directory). */
  def fromTextFile(paths: List[String]): DList[String] = {
    val loader = new Loader[String] {
      def mkInputStore(node: AST.Load[String]) = new InputStore[LongWritable, Text, String](node) {
        private val inputPaths = paths.map(p => new Path(p))

        val inputFormat = classOf[TextInputFormat]

        def inputCheck() = inputPaths foreach { p =>
          if (Helper.pathExists(p))
            logger.info("Input path: " + p.toUri.toASCIIString + " (" + Helper.sizeString(Helper.pathSize(p)) + ")")
          else
             throw new IOException("Input path" + p + " does not exist.")
        }

        def inputConfigure(job: Job) = inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }

        def inputSize(): Long = inputPaths.map(p => Helper.pathSize(p)).sum

        val inputConverter = new InputConverter[LongWritable, Text, String] {
          def fromKeyValue(k: LongWritable, v: Text) = v.toString
        }
      }
    }
    new DList(Smart.Load(loader))
  }

  /** Create a distributed list from a text file that is a number of fields delimited
    * by some separator. Use an extractor function to pull out the required fields to
    * create the distributed list. */
  def extractFromDelimitedTextFile[A : Manifest : WireFormat]
      (sep: String, path: String)
      (extractFn: PartialFunction[List[String], A])
    : DList[A] = {

    val lines = fromTextFile(path)
    lines.flatMap { line =>
      val fields = line.split(sep).toList
      if (extractFn.isDefinedAt(fields)) List(extractFn(fields)) else Nil
    }
  }

  /** Create a distributed list from a text file that is a number of fields delimited
    * by some separator. The type of the resultant list is determined by type inference.
    * An implicit schema must be in scope for the required resultant type. */
  def fromDelimitedTextFile[A : Manifest : Schema : WireFormat]
      (sep: String, path: String)
    : DList[A] = {

    val lines = fromTextFile(path)
    lines.map { line =>
      val fields = line.split(sep).toList
      val (data, _) = implicitly[Schema[A]].read(fields)
      data
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
}


/** A type class for parsing string fields into specific types. */
@annotation.implicitNotFound(msg = "No implicit Schema defined for ${A}.")
trait Schema[A] extends Serializable {
  /* Grab fields, in sequence, required to make 'A', and return the remaining fields. */
  def read(fields: List[String]): (A, List[String])
}

/** Type class instances. */
object Schema {
  implicit object SchemaForUnit extends Schema[Unit] {
    def read(fields: List[String]) = ((), fields)
  }

  implicit object SchemaForBoolean extends Schema[Boolean] {
    def read(fields: List[String]) = (fields(0).toBoolean, fields.drop(1))
  }

  implicit object SchemaForByte extends Schema[Byte] {
    def read(fields: List[String]) = (fields(0).toByte, fields.drop(1))
  }

  implicit object SchemaForChar extends Schema[Char] {
    def read(fields: List[String]) = (fields(0).toArray.apply(0), fields.drop(1))
  }

  implicit object SchemaForDouble extends Schema[Double] {
    def read(fields: List[String]) = (fields(0).toDouble, fields.drop(1))
  }

  implicit object SchemaForFloat extends Schema[Float] {
    def read(fields: List[String]) = (fields(0).toFloat, fields.drop(1))
  }

  implicit object SchemaForLong extends Schema[Long] {
    def read(fields: List[String]) = (fields(0).toLong, fields.drop(1))
  }

  implicit object SchemaForShort extends Schema[Short] {
    def read(fields: List[String]) = (fields(0).toShort, fields.drop(1))
  }

  implicit object SchemaForInt extends Schema[Int] {
    def read(fields: List[String]) = (fields(0).toInt, fields.drop(1))
  }

  implicit object SchemaForString extends Schema[String] {
    def read(fields: List[String]) = (fields(0), fields.drop(1))
  }

  implicit def SchemaForTuple2[T1, T2]
      (implicit sch1: Schema[T1],
                sch2: Schema[T2]) = new Schema[Tuple2[T1, T2]] {

    def read(fields: List[String]) = {
      val (e1, rf1) = sch1.read(fields)
      val (e2, rf2) = sch2.read(rf1)
      ((e1, e2), rf2)
    }
  }

  implicit def SchemaForTuple3[T1, T2, T3]
      (implicit sch1: Schema[T1],
                sch2: Schema[T2],
                sch3: Schema[T3]) = new Schema[Tuple3[T1, T2, T3]] {

    def read(fields: List[String]) = {
      val (e1, rf1) = sch1.read(fields)
      val (e2, rf2) = sch2.read(rf1)
      val (e3, rf3) = sch3.read(rf2)
      ((e1, e2, e3), rf3)
    }
  }

  implicit def SchemaForTuple4[T1, T2, T3, T4]
      (implicit sch1: Schema[T1],
                sch2: Schema[T2],
                sch3: Schema[T3],
                sch4: Schema[T4]) = new Schema[Tuple4[T1, T2, T3, T4]] {

    def read(fields: List[String]) = {
      val (e1, rf1) = sch1.read(fields)
      val (e2, rf2) = sch2.read(rf1)
      val (e3, rf3) = sch3.read(rf2)
      val (e4, rf4) = sch4.read(rf3)
      ((e1, e2, e3, e4), rf4)
    }
  }

  implicit def SchemaForTuple5[T1, T2, T3, T4, T5]
      (implicit sch1: Schema[T1],
                sch2: Schema[T2],
                sch3: Schema[T3],
                sch4: Schema[T4],
                sch5: Schema[T5]) = new Schema[Tuple5[T1, T2, T3, T4, T5]] {

    def read(fields: List[String]) = {
      val (e1, rf1) = sch1.read(fields)
      val (e2, rf2) = sch2.read(rf1)
      val (e3, rf3) = sch3.read(rf2)
      val (e4, rf4) = sch4.read(rf3)
      val (e5, rf5) = sch5.read(rf4)
      ((e1, e2, e3, e4, e5), rf5)
    }
  }
}
