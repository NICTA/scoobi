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
  def objectFromTextFile(paths: String*) = fromTextFile(paths:_*).head

  def fromDelimitedTextFile[A : WireFormat](path: String, sep: String = "\t")
                                           (extractFn: PartialFunction[Seq[String], A]) = TextInput.fromDelimitedTextFile(path, sep)(extractFn)

  def objectFromDelimitedTextFile[A : WireFormat](path: String, sep: String = "\t")
                                                 (extractFn: PartialFunction[Seq[String], A]) =
    fromDelimitedTextFile[A](path, sep)(extractFn).head

  implicit class ListToTextFile[A](val list: core.DList[A]) {
    def toTextFile(path: String, overwrite: Boolean = false) = list.addSink(TextOutput.textFileSink(path, overwrite))
  }
  implicit class ListToDelimitedTextFile[A <: Product : Manifest](val list: core.DList[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false) =
      TextOutput.listToDelimitedTextFile(list, path, sep, overwrite)
  }
  implicit class ObjectToTextFile[A](val o: core.DObject[A]) {
    def toTextFile(path: String, overwrite: Boolean = false) = o.addSink(TextOutput.textFileSink(path, overwrite))
  }
  implicit class ObjectToDelimitedTextFile[A <: Product : Manifest](val o: core.DObject[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false) =
      TextOutput.objectToDelimitedTextFile(o, path, sep, overwrite)
  }
  /** @deprecated(message="use list.toTextFile(...) instead", since="0.7.0") */
  def toTextFile[A : Manifest](dl: core.DList[A], path: String, overwrite: Boolean = false) = TextOutput.toTextFile(dl, path, overwrite)
  /** @deprecated(message="use list.toTextFile(...) instead", since="0.7.0") */
  def toDelimitedTextFile[A <: Product : Manifest](dl: core.DList[A], path: String, sep: String = "\t", overwrite: Boolean = false) = TextOutput.listToDelimitedTextFile(dl, path, sep, overwrite)

  /** Sequence File I/O */
  val SequenceInput = com.nicta.scoobi.io.sequence.SequenceInput
  val SequenceOutput = com.nicta.scoobi.io.sequence.SequenceOutput
  type SeqSchema[A] = com.nicta.scoobi.io.sequence.SeqSchema[A]

  import org.apache.hadoop.io.Writable
  def keyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): core.DList[K] = SequenceInput.keyFromSequenceFile(paths: _*)
  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): core.DObject[K] = keyFromSequenceFile[K](paths:_*).head

  def keyFromSequenceFile[K : WireFormat : SeqSchema](paths: Seq[String], checkKeyType: Boolean = true): core.DList[K] = SequenceInput.keyFromSequenceFile(paths, checkKeyType)
  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: Seq[String], checkKeyType: Boolean = true): core.DObject[K] = keyFromSequenceFile[K](paths, checkKeyType).head

  def valueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): core.DList[V] = SequenceInput.valueFromSequenceFile(paths: _*)
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): core.DObject[V] = valueFromSequenceFile[V](paths:_*).head

  def valueFromSequenceFile[V : WireFormat : SeqSchema](paths: Seq[String], checkValueType: Boolean = true): core.DList[V] = SequenceInput.valueFromSequenceFile(paths, checkValueType)
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: Seq[String], checkValueType: Boolean = true): core.DObject[V] = keyFromSequenceFile[V](paths, checkValueType).head

  def fromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): core.DList[(K, V)] = SequenceInput.fromSequenceFile[K, V](paths)
  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): core.DObject[(K, V)] = fromSequenceFile[K, V](paths:_*).head

  def fromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String], checkKeyValueTypes: Boolean = true): core.DList[(K, V)] =
    SequenceInput.fromSequenceFile[K, V](paths, checkKeyValueTypes)

  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String], checkKeyValueTypes: Boolean = true): core.DObject[(K, V)] =
    fromSequenceFile[K, V](paths, checkKeyValueTypes).head

  /** @deprecated(message="use list.keyToSequenceFile(...) instead", since="0.7.0") */
  def keyToSequenceFile[K : SeqSchema](dl: core.DList[K], path: String, overwrite: Boolean = false) = SequenceOutput.keyToSequenceFile(dl, path, overwrite)
  /** @deprecated(message="use list.valueToSequenceFile(...) instead", since="0.7.0") */
  def valueToSequenceFile[V : SeqSchema](dl: core.DList[V], path: String, overwrite: Boolean = false) = SequenceOutput.valueToSequenceFile(dl, path, overwrite)
  /** @deprecated(message="use list.toSequenceFile(...) instead", since="0.7.0") */
  def toSequenceFile[K, V](dl: core.DList[(K, V)], path: String, overwrite: Boolean = false, checkpoint: Boolean = false)
                          (implicit ks: SeqSchema[K], vs: SeqSchema[V], sc: ScoobiConfiguration) =
    SequenceOutput.toSequenceFile(dl, path, overwrite, checkpoint)(ks, vs, sc)

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
  val AvroInput = com.nicta.scoobi.io.avro.AvroInput
  val AvroOutput = com.nicta.scoobi.io.avro.AvroOutput
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]
  type AvroFixed[A] = com.nicta.scoobi.io.avro.AvroFixed[A]

  def fromAvroFile[A : WireFormat : AvroSchema](paths: String*) = AvroInput.fromAvroFile(paths: _*)(wireFormat[A], implicitly[AvroSchema[A]])
  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: String*) = fromAvroFile[A](paths: _*).head

  def fromAvroFile[A : WireFormat : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true) = AvroInput.fromAvroFile(paths, checkSchemas)(wireFormat[A], implicitly[AvroSchema[A]])
  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true) = fromAvroFile[A](paths, checkSchemas).head

  implicit class ListToAvroFile[A](val list: core.DList[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      list.addSink(AvroOutput.avroSink(path, overwrite, checkpoint))
  }
  implicit class ObjectToAvroFile[A](val o: core.DObject[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      o.addSink(AvroOutput.avroSink(path, overwrite, checkpoint))
  }

  /** @deprecated(message="use list.toAvroFile(...) instead", since="0.7.0") */
  def toAvroFile[B](dl: core.DList[B], path: String, overwrite: Boolean = false, checkpoint: Boolean = false)(implicit schema: AvroSchema[B], sc: ScoobiConfiguration) =
    AvroOutput.toAvroFile(dl, path, overwrite, checkpoint)

}
object InputsOutputs extends InputsOutputs
