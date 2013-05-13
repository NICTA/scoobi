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

import core.{ScoobiConfiguration, WireFormat}
import WireFormat._
import impl.collection._
import Seqs._
import io.text.TextInput
import io.text.TextOutput
import io.avro.AvroInput
import io.avro.AvroOutput
import io.sequence.SequenceInput
import io.sequence.SequenceOutput

/**
 * This trait provides way to create DLists from files
 * and to add sinks to DLists so that the results of computations can be saved to files
 */
trait InputsOutputs extends TextInput with TextOutput with AvroInput with AvroOutput with SequenceInput with SequenceOutput {
  /** Text file I/O */
  def objectFromTextFile(paths: String*) = fromTextFile(paths:_*).head
  def objectFromDelimitedTextFile[A : WireFormat](path: String, sep: String = "\t")
                                                 (extractFn: PartialFunction[Seq[String], A]) =
    fromDelimitedTextFile[A](path, sep)(extractFn).head

  implicit class ListToTextFile[A](val list: core.DList[A]) {
    def toTextFile(path: String, overwrite: Boolean = false) = list.addSink(textFileSink(path, overwrite))
  }
  implicit class ListToDelimitedTextFile[A <: Product : Manifest](val list: core.DList[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false) =
      listToDelimitedTextFile(list, path, sep, overwrite)
  }
  implicit class ObjectToTextFile[A](val o: core.DObject[A]) {
    def toTextFile(path: String, overwrite: Boolean = false) = o.addSink(textFileSink(path, overwrite))
  }
  implicit class ObjectToDelimitedTextFile[A <: Product : Manifest](val o: core.DObject[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false) =
      objectToDelimitedTextFile(o, path, sep, overwrite)
  }
  /** @deprecated(message="use list.toTextFile(...) instead", since="0.7.0") */
  def toDelimitedTextFile[A <: Product : Manifest](dl: core.DList[A], path: String, sep: String = "\t", overwrite: Boolean = false) = listToDelimitedTextFile(dl, path, sep, overwrite)

  /** Sequence File I/O */
  type SeqSchema[A] = com.nicta.scoobi.io.sequence.SeqSchema[A]

  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): core.DObject[K] = keyFromSequenceFile[K](paths:_*).head
  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: Seq[String], checkKeyType: Boolean = true): core.DObject[K] = keyFromSequenceFile[K](paths, checkKeyType).head
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): core.DObject[V] = valueFromSequenceFile[V](paths:_*).head
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: Seq[String], checkValueType: Boolean = true): core.DObject[V] = SequenceInput.keyFromSequenceFile[V](paths, checkValueType).head
  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): core.DObject[(K, V)] = SequenceInput.fromSequenceFile[K, V](paths).head
  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String], checkKeyValueTypes: Boolean = true): core.DObject[(K, V)] =
    fromSequenceFile[K, V](paths, checkKeyValueTypes).head

  implicit class ConvertKeyListToSequenceFile[K](val list: core.DList[K]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false)(implicit ks: SeqSchema[K]) =
      list.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertKeyListToSequenceFile1[K, V](val list: core.DList[(K, V)]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false)(implicit ks: SeqSchema[K]) =
      list.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertKeyObjectToSequenceFile[K](val o: core.DObject[K]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false)(implicit ks: SeqSchema[K]) =
      o.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertValueListToSequenceFile[V](val list: core.DList[V]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false)(implicit vs: SeqSchema[V]) =
      list.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertValueListToSequenceFile1[K, V](val list: core.DList[(K, V)]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false)(implicit vs: SeqSchema[V]) =
      list.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertValueObjectToSequenceFile[V](val o: core.DObject[V]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false)(implicit vs: SeqSchema[V]) =
      o.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite))
  }
  implicit class ConvertListToSequenceFile[T](val list: core.DList[T]) {
    def toSequenceFile[K, V](path: String, overwrite: Boolean = false, checkpoint: Boolean = false)
      (implicit ev: T <:< (K, V), ks: SeqSchema[K], vs: SeqSchema[V], sc: ScoobiConfiguration)
        = list.addSink(SequenceOutput.schemaSequenceSink(path, overwrite, checkpoint)(ks, vs, sc))
  }
  implicit class ConvertObjectToSequenceFile[T](val o: core.DObject[T]) {
    def toSequenceFile[K, V](path: String, overwrite: Boolean = false, checkpoint: Boolean = false)
                                   (implicit ev: T <:< (K, V), ks: SeqSchema[K], vs: SeqSchema[V], sc: ScoobiConfiguration)
    = o.addSink(SequenceOutput.schemaSequenceSink(path, overwrite)(ks, vs, sc))
  }

  /** Avro I/O */
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]
  type AvroFixed[A] = com.nicta.scoobi.io.avro.AvroFixed[A]

  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: String*) = fromAvroFile[A](paths: _*).head
  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true) = fromAvroFile[A](paths, checkSchemas).head

  implicit class ListToAvroFile[A](val list: core.DList[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      list.addSink(AvroOutput.avroSink(path, overwrite, checkpoint))
  }
  implicit class ObjectToAvroFile[A](val o: core.DObject[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      o.addSink(AvroOutput.avroSink(path, overwrite, checkpoint))
  }
}
object InputsOutputs extends InputsOutputs
