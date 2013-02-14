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
package application

import core.WireFormat
import WireFormat._

/**
 * This trait provides way to create DLists from files
 * and to add sinks to DLists so that the results of computations can be saved to files
 */
trait InputsOutputs {
  /** Text file I/O */
  val TextOutput = com.nicta.scoobi.io.text.TextOutput
  val TextInput = com.nicta.scoobi.io.text.TextInput
  val AnInt = TextInput.AnInt
  val ALong = TextInput.ALong
  val ADouble = TextInput.ADouble
  val AFloat = TextInput.AFloat

  def fromTextFile(paths: String*) = TextInput.fromTextFile(paths: _*)
  def fromTextFile(paths: List[String]) = TextInput.fromTextFile(paths)
  def fromDelimitedTextFile[A : WireFormat]
  (path: String, sep: String = "\t")
  (extractFn: PartialFunction[List[String], A]) = TextInput.fromDelimitedTextFile(path, sep)(extractFn)

  implicit def listToTextFile[A](list: core.DList[A]): ListToTextFile[A] = new ListToTextFile[A](list)
  case class ListToTextFile[A](list: core.DList[A]) {
    def toTextFile(path: String, overwrite: Boolean = false) = list.addSink(TextOutput.textFileSink(path, overwrite))
  }
  def toTextFile[A : Manifest](dl: core.DList[A], path: String, overwrite: Boolean = false) = TextOutput.toTextFile(dl, path, overwrite)
  def toDelimitedTextFile[A <: Product : Manifest](dl: core.DList[A], path: String, sep: String = "\t", overwrite: Boolean = false) = TextOutput.toDelimitedTextFile(dl, path, sep, overwrite)

  /** Sequence File I/O */
  val SequenceInput = com.nicta.scoobi.io.sequence.SequenceInput
  val SequenceOutput = com.nicta.scoobi.io.sequence.SequenceOutput
  type SeqSchema[A] = com.nicta.scoobi.io.sequence.SeqSchema[A]

  import org.apache.hadoop.io.Writable
  def convertKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): core.DList[K] = SequenceInput.convertKeyFromSequenceFile(paths: _*)
  def convertKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: List[String], checkKeyType: Boolean = true): core.DList[K] = SequenceInput.convertKeyFromSequenceFile(paths, checkKeyType)
  def convertValueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): core.DList[V] = SequenceInput.convertValueFromSequenceFile(paths: _*)
  def convertValueFromSequenceFile[V : WireFormat : SeqSchema](paths: List[String], checkValueType: Boolean = true): core.DList[V] = SequenceInput.convertValueFromSequenceFile(paths, checkValueType)
  def convertFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): core.DList[(K, V)] = SequenceInput.convertFromSequenceFile(paths: _*)
  def convertFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: List[String], checkKeyValueTypes: Boolean = true): core.DList[(K, V)] = SequenceInput.convertFromSequenceFile(paths, checkKeyValueTypes)(wireFormat[K], implicitly[SeqSchema[K]], wireFormat[V], implicitly[SeqSchema[V]])
  def fromSequenceFile[K <: Writable : WireFormat : Manifest, V <: Writable : WireFormat : Manifest](paths: String*): core.DList[(K, V)] =
    SequenceInput.fromSequenceFile(paths: _*)(wireFormat[K], implicitly[Manifest[K]],  wireFormat[V], implicitly[Manifest[V]])
  def fromSequenceFile[K <: Writable : WireFormat : Manifest, V <: Writable : WireFormat : Manifest](paths: Seq[String], checkKeyValueTypes: Boolean = true): core.DList[(K, V)] =
    SequenceInput.fromSequenceFile(paths, checkKeyValueTypes)(wireFormat[K], implicitly[Manifest[K]],  wireFormat[V], implicitly[Manifest[V]])

  def convertKeyToSequenceFile[K : SeqSchema](dl: core.DList[K], path: String, overwrite: Boolean = false) = SequenceOutput.convertKeyToSequenceFile(dl, path, overwrite)
  def convertValueToSequenceFile[V : SeqSchema](dl: core.DList[V], path: String, overwrite: Boolean = false) = SequenceOutput.convertValueToSequenceFile(dl, path, overwrite)
  def convertToSequenceFile[K : SeqSchema, V : SeqSchema](dl: core.DList[(K, V)], path: String, overwrite: Boolean = false) = SequenceOutput.convertToSequenceFile(dl, path, overwrite)

  implicit def convertKeyListToSequenceFile[K : SeqSchema](list: core.DList[K]): ConvertKeyListToSequenceFile[K] = new ConvertKeyListToSequenceFile[K](list)
  case class ConvertKeyListToSequenceFile[K : SeqSchema](list: core.DList[K]) {
    def convertKeyToSequenceFile(path: String, overwrite: Boolean = false) =
      list.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite))
  }

  implicit def convertValueListToSequenceFile[V : SeqSchema](list: core.DList[V]): ConvertValueListToSequenceFile[V] = new ConvertValueListToSequenceFile[V](list)
  case class ConvertValueListToSequenceFile[V : SeqSchema](list: core.DList[V]) {
    def convertValueToSequenceFile(path: String, overwrite: Boolean = false) =
      list.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite))
  }

  implicit def convertListToSequenceFile[K : SeqSchema, V : SeqSchema](list: core.DList[(K, V)]): ConvertListToSequenceFile[K, V] = new ConvertListToSequenceFile[K, V](list)
  case class ConvertListToSequenceFile[K : SeqSchema, V : SeqSchema](list: core.DList[(K, V)]) {
    def convertToSequenceFile(path: String, overwrite: Boolean = false) =
      list.addSink(SequenceOutput.schemaSequenceSink(path, overwrite)(implicitly[SeqSchema[K]], implicitly[SeqSchema[V]]))
  }

  implicit def listToSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](list: core.DList[(K, V)]): ListToSequenceFile[K, V] = new ListToSequenceFile[K, V](list)
  case class ListToSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](list: core.DList[(K, V)]) {
    def toSequenceFile(path: String, overwrite: Boolean = false) =
      list.addSink(SequenceOutput.sequenceSink[K, V](path, overwrite))
  }

  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: core.DList[(K, V)], path: String, overwrite: Boolean = false) = SequenceOutput.toSequenceFile(dl, path, overwrite)

  /** Avro I/O */
  val AvroInput = com.nicta.scoobi.io.avro.AvroInput
  val AvroOutput = com.nicta.scoobi.io.avro.AvroOutput
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]
  type AvroFixed[A] = com.nicta.scoobi.io.avro.AvroFixed[A]

  def fromAvroFile[A : WireFormat : AvroSchema](paths: String*) = AvroInput.fromAvroFile(paths: _*)(wireFormat[A], implicitly[AvroSchema[A]])
  def fromAvroFile[A : WireFormat : AvroSchema](paths: List[String], checkSchemas: Boolean = true) = AvroInput.fromAvroFile(paths, checkSchemas)(wireFormat[A], implicitly[AvroSchema[A]])

  implicit def listToAvroFile[A : AvroSchema](list: core.DList[A]): ListToAvroFile[A] = new ListToAvroFile[A](list)
  case class ListToAvroFile[A : AvroSchema](list: core.DList[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false) = list.addSink(AvroOutput.avroSink(path, overwrite))
  }

  def toAvroFile[B : AvroSchema](dl: core.DList[B], path: String, overwrite: Boolean = false) = AvroOutput.toAvroFile(dl, path, overwrite)
}
object InputsOutputs extends InputsOutputs
