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
package sequence

import java.io.IOException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Writable, SequenceFile}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.Job

import core._
import WireFormat._
import impl.plan.DListImpl
import impl.ScoobiConfiguration._
import impl.io.Helper
import impl.util.Compatibility

/** Smart functions for materialising distributed lists by loading Sequence files. */
trait SequenceInput {

  def defaultSequenceInputFormat[K, V] = classOf[SequenceFileInputFormat[K, V]]

  /** Create a new DList from the "key" contents of one or more Sequence Files. Note that the type parameter K
    * is the "converted" Scala type for the Writable key type that must be contained in the the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def keyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): DList[K] =
    keyFromSequenceFile(paths, checkKeyType = true)

  /** Create a new DList from the "key" contents of a list of one or more Sequence Files. Note that the type parameter
    * K is the "converted" Scala type for the Writable key type that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def keyFromSequenceFile[K : WireFormat : SeqSchema](paths: Seq[String], checkKeyType: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck): DList[K] = {
    val convK = implicitly[SeqSchema[K]]

    val converter = new InputConverter[convK.SeqType, Writable, K] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: Writable) = convK.fromWritable(k)
    }

    fromSequenceSource(new CheckedSeqSource[convK.SeqType, Writable, K](paths,
      defaultSequenceInputFormat[convK.SeqType, Writable],
      converter,
      checkKeyType,
      check)(convK.mf, implicitly[Manifest[Writable]]))
  }

  /** Create a new DList from the "value" contents of one or more Sequence Files. Note that the type parameter V
    * is the "converted" Scala type for the Writable value type that must be contained in the the Sequence
    * Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def valueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): DList[V] =
    valueFromSequenceFile(paths, checkValueType = true)


  /** Create a new DList from the "value" contents of a list of one or more Sequence Files. Note that the type parameter
    * V is the "converted" Scala type for the Writable value type that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def valueFromSequenceFile[V : WireFormat : SeqSchema](paths: Seq[String], checkValueType: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck): DList[V] = {
    val convV = implicitly[SeqSchema[V]]

    val converter = new InputConverter[Writable, convV.SeqType, V] {
      def fromKeyValue(context: InputContext, k: Writable, v: convV.SeqType) = convV.fromWritable(v)
    }

    fromSequenceSource(new CheckedSeqSource[Writable, convV.SeqType, V](
      paths,
      defaultSequenceInputFormat[Writable, convV.SeqType],
      converter,
      checkValueType,
      check)(implicitly[Manifest[Writable]], convV.mf))
  }

  def fromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): DList[(K, V)] =
    fromSequenceFile(paths, checkKeyValueTypes = true)

  /** Create a new DList from the contents of a list of one or more Sequence Files. Note that the type parameters
    * K and V are the "converted" Scala types for the Writable key-value types that must be contained in the the
    * Sequence Files. In the case of a directory being specified, the input forms all the files in that directory. */
  def fromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String], checkKeyValueTypes: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck): DList[(K, V)] = {

    val convK = implicitly[SeqSchema[K]]
    val convV = implicitly[SeqSchema[V]]

    val converter = new InputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: convV.SeqType) = (convK.fromWritable(k), convV.fromWritable(v))
    }

    fromSequenceSource(new CheckedSeqSource[convK.SeqType, convV.SeqType, (K, V)](paths, defaultSequenceInputFormat, converter, checkKeyValueTypes, check)(convK.mf, convV.mf))
  }

  def fromSequenceSource[K, V, A : WireFormat](source: SeqSource[K, V, A]) = DListImpl[A](source)

  def checkedSource[K : Manifest, V : Manifest](paths: Seq[String], checkKeyValueTypes: Boolean = true) = {
    val converter = new InputConverter[K, V, (K, V)] {
      def fromKeyValue(context: InputContext, k: K, v: V) = (k, v)
    }
    new CheckedSeqSource[K, V, (K, V)](paths, defaultSequenceInputFormat, converter, checkKeyValueTypes)
  }

  def source[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String]) = {
    val converter = new InputConverter[K, V, (K, V)] {
      def fromKeyValue(context: InputContext, k: K, v: V) = (k, v)
    }
    new SeqSource[K, V, (K, V)](paths, defaultSequenceInputFormat[K, V], converter)
  }

  def valueSource[V : SeqSchema](paths: Seq[String]) = {
    val convV = implicitly[SeqSchema[V]]

    val converter = new InputConverter[Writable, convV.SeqType, V] {
      def fromKeyValue(context: InputContext, k: Writable, v: convV.SeqType) = convV.fromWritable(v)
    }
    new SeqSource(paths, defaultSequenceInputFormat, converter)
  }

}
object SequenceInput extends SequenceInput

/* Class that abstracts all the common functionality of reading from sequence files. */
class SeqSource[K, V, A](paths: Seq[String],
                         val inputFormat: Class[SequenceFileInputFormat[K, V]] = SequenceInput.defaultSequenceInputFormat,
                         val inputConverter: InputConverter[K, V, A],
                         checkFileTypes: Boolean = true,
                         val check: Source.InputCheck = Source.defaultInputCheck)
  extends DataSource[K, V, A] {

  private val inputPaths = paths.map(p => new Path(p))

  override def toString = "SeqSource("+id+")"+inputPaths.mkString("\n", "\n", "\n")

  /** Check if the input path exists, and optionally the expected key/value types match those in the file.
    * For efficiency, the type checking will only check one file per dir */
  def inputCheck(implicit sc: ScoobiConfiguration) {
    check(inputPaths, sc)
    inputPaths foreach checkInputPathType
  }

  protected def checkInputPathType(p: Path)(implicit sc: ScoobiConfiguration) {}

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = inputPaths.map(p => Helper.pathSize(p)(sc)).sum
}

/** This class can check if the source types are ok, based on the Manifest of the input types */
class CheckedSeqSource[K : Manifest, V : Manifest, A](paths: Seq[String],
                                                      override val inputFormat: Class[SequenceFileInputFormat[K, V]] = SequenceInput.defaultSequenceInputFormat,
                                                      override val inputConverter: InputConverter[K, V, A], checkFileTypes: Boolean = true,
                                                      override val check: Source.InputCheck = Source.defaultInputCheck) extends
   SeqSource[K, V, A](paths, inputFormat, inputConverter, checkFileTypes, check) {

  override protected def checkInputPathType(p: Path)(implicit sc: ScoobiConfiguration) {
    if (checkFileTypes)
      Helper.getSingleFilePerDir(p)(sc) foreach { filePath =>
        val seqReader: SequenceFile.Reader = Compatibility.newSequenceFileReader(sc, filePath)
        checkType(seqReader.getKeyClass, manifest[K].runtimeClass, "KEY")
        checkType(seqReader.getValueClass, manifest[V].runtimeClass, "VALUE")
      }
  }

  private def checkType(actual: Class[_], expected: Class[_], typeStr: String) {
    val msg = "Incompatible %s type in SequenceFile. Expecting '%s' to be equal to or a subclass of '%s'."
    if (!expected.isAssignableFrom(actual)) throw new IOException(msg.format(typeStr, expected, actual))
  }
}

