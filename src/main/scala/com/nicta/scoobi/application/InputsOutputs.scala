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

import com.nicta.scoobi.core._
import WireFormat._
import impl.collection._
import Seqs._
import impl.plan.source.SeqInput
import io.text.TextInput
import io.text.TextOutput
import io.avro.AvroInput
import io.avro.AvroOutput
import io.sequence.SequenceInput
import io.sequence.SequenceOutput
import impl.mapreducer.BridgeStore
import java.util.UUID
import scala.Some
import com.nicta.scoobi.impl.plan.DObjectImpl
import com.nicta.scoobi.impl.plan.comp._
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.impl.io.GlobIterator
import com.nicta.scoobi.impl.rtt.ScoobiWritable

/**
 * This trait provides way to create DLists from files
 * and to add sinks to DLists so that the results of computations can be saved to files
 */
trait InputsOutputs extends TextInput with TextOutput with AvroInput with AvroOutput with SequenceInput with SequenceOutput {

  /** create a DList from a stream of elements which will only be evaluated on the cluster */
  def fromLazySeq[A : WireFormat](seq: =>Seq[A], seqSize: Int = 1000): core.DList[A] = SeqInput.fromLazySeq(() => seq, seqSize)
  /** create a DObject which will only be evaluated on the cluster */
  def lazyObject[A : WireFormat](o: =>A): core.DObject[A] = fromLazySeq(Seq(o), seqSize = 1).materialise.map(_.head)
  /** Text file I/O */
  def objectFromTextFile(paths: String*): core.DObject[String] = {
    if (paths.size == 1) new DObjectImpl(ReturnSC({ sc: ScoobiConfiguration => implicit val configuration = sc.configuration
        new GlobIterator(new Path(paths.head), GlobIterator.sourceIterator).toSeq
      })).map((_: Seq[String]).head)
    else fromTextFile(paths:_*).head
  }

  def objectFromDelimitedTextFile[A : WireFormat](path: String, sep: String = "\t", check: Source.InputCheck = Source.defaultInputCheck)
                                                 (extractFn: PartialFunction[Seq[String], A]) = {

    new DObjectImpl(ReturnSC({ sc: ScoobiConfiguration => implicit val configuration = sc.configuration
      check(Seq(new Path(path)), sc)
      new GlobIterator(new Path(path), GlobIterator.sourceIterator).toSeq.map(line => extractFn(line.split(sep)))
    })).map((_:Seq[A]).head)
  }

  implicit class ListToTextFile[A](val list: core.DList[A]) {
    def toTextFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) = list.addSink(textFileSink(path, overwrite, check))
  }
  implicit class ListToDelimitedTextFile[A <: Product : Manifest](val list: core.DList[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
      listToDelimitedTextFile(list, path, sep, overwrite, check)
  }
  implicit class ObjectToTextFile[A](val o: core.DObject[A]) {
    def toTextFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
      o.addSink(textFileSink(path, overwrite, check))
  }
  implicit class ObjectToDelimitedTextFile[A <: Product : Manifest](val o: core.DObject[A]) {
    def toDelimitedTextFile(path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
      objectToDelimitedTextFile(o, path, sep, overwrite, check)
  }
  /** @deprecated(message="use list.toTextFile(...) instead", since="0.7.0") */
  def toDelimitedTextFile[A <: Product : Manifest](dl: core.DList[A], path: String, sep: String = "\t", overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck) =
    listToDelimitedTextFile(dl, path, sep, overwrite, check)

  /** Sequence File I/O */
  type SeqSchema[A] = com.nicta.scoobi.io.sequence.SeqSchema[A]

  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: String*): core.DObject[K] = {
    if (paths.size == 1) new DObjectImpl(ReturnSC({ sc: ScoobiConfiguration => implicit val configuration = sc.configuration
      new GlobIterator(new Path(paths.head+"/*"), GlobIterator.keySequenceIterator(configuration, implicitly[WireFormat[K]], implicitly[SeqSchema[K]])).toSeq
    })).map((_: Seq[K]).head)
    else keyFromSequenceFile[K](paths:_*).head
  }
  def objectKeyFromSequenceFile[K : WireFormat : SeqSchema](paths: Seq[String], checkKeyType: Boolean = true): core.DObject[K] = keyFromSequenceFile[K](paths, checkKeyType).head
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: String*): core.DObject[V] = {
    if (paths.size == 1) new DObjectImpl(ReturnSC({ sc: ScoobiConfiguration => implicit val configuration = sc.configuration
      new GlobIterator(new Path(paths.head+"/*"), GlobIterator.valueSequenceIterator(configuration, implicitly[WireFormat[V]], implicitly[SeqSchema[V]])).toSeq
    })).map((_: Seq[V]).head)
    else valueFromSequenceFile[V](paths:_*).head
  }
  def objectValueFromSequenceFile[V : WireFormat : SeqSchema](paths: Seq[String], checkValueType: Boolean = true): core.DObject[V] = SequenceInput.keyFromSequenceFile[V](paths, checkValueType).head
  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: String*): core.DObject[(K, V)] = {
    if (paths.size == 1) new DObjectImpl(ReturnSC({ sc: ScoobiConfiguration => implicit val configuration = sc.configuration
      new GlobIterator(new Path(paths.head+"/*"), GlobIterator.sequenceIterator(configuration, implicitly[WireFormat[K]], implicitly[SeqSchema[K]], implicitly[WireFormat[V]], implicitly[SeqSchema[V]])).toSeq
    })).map((_: Seq[(K, V)]).head)
    else SequenceInput.fromSequenceFile[K, V](paths).head
  }
  def objectFromSequenceFile[K : WireFormat : SeqSchema, V : WireFormat : SeqSchema](paths: Seq[String], checkKeyValueTypes: Boolean = true): core.DObject[(K, V)] =
    fromSequenceFile[K, V](paths, checkKeyValueTypes).head

  implicit class ConvertKeyListToSequenceFile[K](val list: core.DList[K]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit ks: SeqSchema[K]) =
      list.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite, check))
  }
  implicit class ConvertKeyListToSequenceFile1[K, V](val list: core.DList[(K, V)]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit ks: SeqSchema[K]) =
      list.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite, check))
  }
  implicit class ConvertKeyObjectToSequenceFile[K](val o: core.DObject[K]) {
    def keyToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit ks: SeqSchema[K]) =
      o.addSink(SequenceOutput.keySchemaSequenceFile(path, overwrite, check))
  }
  implicit class ConvertValueListToSequenceFile[V](val list: core.DList[V]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit vs: SeqSchema[V], sc: ScoobiConfiguration) =
      list.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite, check, checkpoint, expiryPolicy))
  }
  implicit class ConvertValueListToSequenceFile1[K, V](val list: core.DList[(K, V)]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit vs: SeqSchema[V], sc: ScoobiConfiguration) =
      list.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite, check))
  }
  implicit class ConvertValueObjectToSequenceFile[V](val o: core.DObject[V]) {
    def valueToSequenceFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit vs: SeqSchema[V], sc: ScoobiConfiguration) =
      o.addSink(SequenceOutput.valueSchemaSequenceFile(path, overwrite, check, checkpoint, expiryPolicy))
  }
  implicit class ConvertListToSequenceFile[T](val list: core.DList[T]) {
    def toSequenceFile[K, V](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)
      (implicit ev: T <:< (K, V), ks: SeqSchema[K], vs: SeqSchema[V], sc: ScoobiConfiguration)
        = list.addSink(SequenceOutput.schemaSequenceSink(path, overwrite, check, checkpoint, expiryPolicy)(ks, vs, sc))
  }
  implicit class ConvertObjectToSequenceFile[T](val o: core.DObject[T]) {
    def toSequenceFile[K, V](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)
                                   (implicit ev: T <:< (K, V), ks: SeqSchema[K], vs: SeqSchema[V], sc: ScoobiConfiguration)
    = o.addSink(SequenceOutput.schemaSequenceSink(path, overwrite, check, checkpoint, expiryPolicy)(ks, vs, sc))
  }

  /** Avro I/O */
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]
  type AvroFixed[A] = com.nicta.scoobi.io.avro.AvroFixed[A]

  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: String*) = fromAvroFile[A](paths: _*).head
  def objectFromAvroFile[A : WireFormat : AvroSchema](paths: Seq[String], checkSchemas: Boolean = true) = fromAvroFile[A](paths, checkSchemas).head

  implicit class ListToAvroFile[A](val list: core.DList[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      list.addSink(AvroOutput.avroSink(path, overwrite, check, checkpoint, expiryPolicy))
  }
  implicit class ObjectToAvroFile[A](val o: core.DObject[A]) {
    def toAvroFile(path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck, checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit as: AvroSchema[A], sc: ScoobiConfiguration) =
      o.addSink(AvroOutput.avroSink(path, overwrite, check, checkpoint, expiryPolicy))
  }

  /** checkpoints */
  implicit class ListToCheckpointFile[A](val list: core.DList[A]) {
    def checkpoint(path: String, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit sc: ScoobiConfiguration) =
      list.addSink(new BridgeStore(UUID.randomUUID.toString, list.wf, Checkpoint.create(Some(path), expiryPolicy, doIt = true)))
  }

}
object InputsOutputs extends InputsOutputs
