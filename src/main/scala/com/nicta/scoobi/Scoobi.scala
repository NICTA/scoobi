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
package com.nicta.scoobi

/** Global Scoobi functions and values. */
object Scoobi extends com.nicta.scoobi.WireFormatImplicits with com.nicta.scoobi.GroupingImplicits {

  /* Primary types */
  type WireFormat[A] = com.nicta.scoobi.WireFormat[A]
  val DList = com.nicta.scoobi.DList
  type DList[A] = com.nicta.scoobi.DList[A]
  implicit def travPimp[A : Manifest : WireFormat](trav: Traversable[A]) = DList.travPimp(trav)

  val DObject = com.nicta.scoobi.DObject
  type DObject[A] = com.nicta.scoobi.DObject[A]

  type DoFn[A, B] = com.nicta.scoobi.DoFn[A, B]
  type BasicDoFn[A, B] = com.nicta.scoobi.BasicDoFn[A, B]
  type EnvDoFn[A, B, E] = com.nicta.scoobi.EnvDoFn[A, B, E]

  val Grouping = com.nicta.scoobi.Grouping
  type Grouping[A] = com.nicta.scoobi.Grouping[A]

  type ScoobiApp = com.nicta.scoobi.ScoobiApp

  /* Persisting */
  def persist[P](p: P)(implicit conf: ScoobiConfiguration, persister: Persister[P]): persister.Out = Persist.persist(p)(conf, persister)
  def persist[P](conf: ScoobiConfiguration)(p: P)(implicit persister: Persister[P]): persister.Out = Persist.persist(p)(conf, persister)
  val Persister = com.nicta.scoobi.Persister


  /* Text file I/O */
  val TextOutput = com.nicta.scoobi.io.text.TextOutput
  val TextInput = com.nicta.scoobi.io.text.TextInput
  val AnInt = TextInput.AnInt
  val ALong = TextInput.ALong
  val ADouble = TextInput.ADouble
  val AFloat = TextInput.AFloat

  def fromTextFile(paths: String*) = TextInput.fromTextFile(paths: _*)
  def fromTextFile(paths: List[String]) = TextInput.fromTextFile(paths)
  def fromDelimitedTextFile[A : Manifest : WireFormat]
      (path: String, sep: String = "\t")
      (extractFn: PartialFunction[List[String], A]) = TextInput.fromDelimitedTextFile(path, sep)(extractFn)
  def toTextFile[A : Manifest](dl: DList[A], path: String, overwrite: Boolean = false) = TextOutput.toTextFile(dl, path, overwrite)
  def toDelimitedTextFile[A <: Product : Manifest](dl: DList[A], path: String, sep: String = "\t", overwrite: Boolean = false) = TextOutput.toDelimitedTextFile(dl, path, sep, overwrite)


  /* Sequence File I/O */
  val SequenceInput = com.nicta.scoobi.io.seq.SequenceInput
  val SequenceOutput = com.nicta.scoobi.io.seq.SequenceOutput
  type SeqSchema[A] = com.nicta.scoobi.io.seq.SeqSchema[A]

  import org.apache.hadoop.io.Writable
  def convertKeyFromSequenceFile[K : Manifest : WireFormat : SeqSchema](paths: String*): DList[K] = SequenceInput.convertKeyFromSequenceFile(paths: _*)
  def convertKeyFromSequenceFile[K : Manifest : WireFormat : SeqSchema](paths: List[String]): DList[K] = SequenceInput.convertKeyFromSequenceFile(paths)
  def convertValueFromSequenceFile[V : Manifest : WireFormat : SeqSchema](paths: String*): DList[V] = SequenceInput.convertValueFromSequenceFile(paths: _*)
  def convertValueFromSequenceFile[V : Manifest : WireFormat : SeqSchema](paths: List[String]): DList[V] = SequenceInput.convertValueFromSequenceFile(paths)
  def convertFromSequenceFile[K : Manifest : WireFormat : SeqSchema, V : Manifest : WireFormat : SeqSchema](paths: String*): DList[(K, V)] = SequenceInput.convertFromSequenceFile(paths: _*)
  def convertFromSequenceFile[K : Manifest : WireFormat : SeqSchema, V : Manifest : WireFormat : SeqSchema](paths: List[String]): DList[(K, V)] = SequenceInput.convertFromSequenceFile(paths)
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: String*): DList[(K, V)] = SequenceInput.fromSequenceFile(paths: _*)
  def fromSequenceFile[K <: Writable : Manifest : WireFormat, V <: Writable : Manifest : WireFormat](paths: List[String]): DList[(K, V)] = SequenceInput.fromSequenceFile(paths)

  def convertKeyToSequenceFile[K : SeqSchema](dl: DList[K], path: String, overwrite: Boolean = false): DListPersister[K] = SequenceOutput.convertKeyToSequenceFile(dl, path, overwrite)
  def convertValueToSequenceFile[V : SeqSchema](dl: DList[V], path: String, overwrite: Boolean = false): DListPersister[V] = SequenceOutput.convertValueToSequenceFile(dl, path, overwrite)
  def convertToSequenceFile[K : SeqSchema, V : SeqSchema](dl: DList[(K, V)], path: String, overwrite: Boolean = false): DListPersister[(K, V)] = SequenceOutput.convertToSequenceFile(dl, path, overwrite)
  def toSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](dl: DList[(K, V)], path: String, overwrite: Boolean = false): DListPersister[(K, V)] = SequenceOutput.toSequenceFile(dl, path, overwrite)


  /* Avro I/O */
  val AvroInput = com.nicta.scoobi.io.avro.AvroInput
  val AvroOutput = com.nicta.scoobi.io.avro.AvroOutput
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]

  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: String*) = AvroInput.fromAvroFile(paths: _*)
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: List[String]) = AvroInput.fromAvroFile(paths)
  def toAvroFile[B : AvroSchema](dl: DList[B], path: String, overwrite: Boolean = false) = AvroOutput.toAvroFile(dl, path, overwrite)
  
  /* lib stuff */
  
  implicit def dlistToRelational[K: Manifest: WireFormat: Grouping, A: Manifest: WireFormat](dl: DList[(K, A)]): com.nicta.scoobi.lib.Relational[K,A] = com.nicta.scoobi.lib.Relational(dl)
  implicit def relationalToDList[K, A](r: com.nicta.scoobi.lib.Relational[K, A]): DList[(K,A)] = r.left
  
  import com.nicta.scoobi.lib.LinearAlgebra._
  import com.nicta.scoobi.lib.DVector
  import com.nicta.scoobi.lib.InMemDenseVector
  import com.nicta.scoobi.lib.DRowWiseMatrix
  import com.nicta.scoobi.lib.DColWiseMatrix
  import com.nicta.scoobi.lib.InMemVector
  import com.nicta.scoobi.lib.DMatrix
  
  implicit def dlistToDVector[Elem: Manifest: WireFormat: Ordering, V: Manifest: WireFormat: Ordering](v: DList[(Elem, V)]) = DVector(v)
  implicit def dvectorToDList[Elem, V](v: DVector[Elem, V]) = v.data
  
  implicit def inMemDenseVectorToDObject[T](in: InMemDenseVector[T]) = in.data
  
   /**
   * Note this is an expensive conversion (it adds an extra map-reduce job), try save the result to reuse if applicable
   */
  implicit def dlistToRowWiseWithMapReduceJob[E: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DMatrix[E, T]): DRowWiseMatrix[E, T] =
    DRowWiseMatrix(m.map { case ((r, c), v) => (r, (c, v)) }.groupByKey)

  implicit def dlistToRowWise[Elem: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DList[(Elem, Iterable[(Elem, T)])]): DRowWiseMatrix[Elem, T] =
    DRowWiseMatrix(m)

  implicit def rowWiseToDList[Elem: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DRowWiseMatrix[Elem, T]) = m.data

  
  implicit def dlistToDMatrix[Elem: Manifest: WireFormat: Ordering, Value: Manifest: WireFormat](
    v: DList[((Elem, Elem), Value)]): DMatrix[Elem, Value] =
    DMatrix[Elem, Value](v)
    
  implicit def dmatrixToDlist[Elem: Manifest: WireFormat: Ordering, Value: Manifest: WireFormat](v: DMatrix[Elem, Value]): DList[((Elem, Elem), Value)] = v.data
  
  /**
   * Note this is an expensive conversion (it adds an extra map-reduce job), try save the result to reuse if applicable.
   */
  implicit def dlistToColWiseWithMapReduceJob[Elem: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DMatrix[Elem, T]): DColWiseMatrix[Elem, T] =
    DColWiseMatrix(m.map { case ((r, c), v) => (c, (r, v)) }.groupByKey)

  implicit def dlistToColWise[Elem: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DList[(Elem, Iterable[(Elem, T)])]): DColWiseMatrix[Elem, T] =
    DColWiseMatrix(m)

  implicit def colWiseToDList[Elem: Manifest: WireFormat: Ordering, T: Manifest: WireFormat](m: DColWiseMatrix[Elem, T]) = m.data
  
  
  implicit def inMemVectorToDObject[Elem, T](in: InMemVector[Elem, T]) = in.data



}
