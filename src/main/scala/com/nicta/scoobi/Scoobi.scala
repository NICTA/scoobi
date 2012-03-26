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
object Scoobi extends com.nicta.scoobi.WireFormatImplicits
  with com.nicta.scoobi.io.seq.SequenceOutputConversions with com.nicta.scoobi.io.seq.SequenceInputConversions  {

  val DList = com.nicta.scoobi.DList
  type DList[A] = com.nicta.scoobi.DList[A]
  type DObject[A] = com.nicta.scoobi.DObject[A]
  val DoFn = com.nicta.scoobi.DoFn
  type DoFn[A, B] = com.nicta.scoobi.DoFn[A, B]
  val Grouping = com.nicta.scoobi.Grouping
  type Grouping[A] = com.nicta.scoobi.Grouping[A]
  type Job = com.nicta.scoobi.Job
  val Job = com.nicta.scoobi.Job
  type ScoobiApp = com.nicta.scoobi.ScoobiApp
  val Conf = com.nicta.scoobi.Conf

  type WireFormat[A] = com.nicta.scoobi.WireFormat[A]

  val TextOutput = com.nicta.scoobi.io.text.TextOutput
  val TextInput = com.nicta.scoobi.io.text.TextInput
  val Int = TextInput.Int
  val Long = TextInput.Long
  val Double = TextInput.Double

  val SequenceOutput = com.nicta.scoobi.io.seq.SequenceOutput
  val SequenceInput = com.nicta.scoobi.io.seq.SequenceInput

  val AvroInput = com.nicta.scoobi.io.avro.AvroInput
  val AvroOutput = com.nicta.scoobi.io.avro.AvroOutput
  val AvroSchema = com.nicta.scoobi.io.avro.AvroSchema
  type AvroSchema[A] = com.nicta.scoobi.io.avro.AvroSchema[A]

  val Join = com.nicta.scoobi.lib.Join

  // dlist
  def persist = DList.persist _

  // conf functions
  def getWorkingDirectory = Conf.getWorkingDirectory _
  def conf = Conf.conf
  def jobId = Conf.jobId
  def getUserJars = Conf.getUserJars
  def withHadoopArgs = Conf.withHadoopArgs _

  // text file functions
  def fromTextFile(paths: String*) = TextInput.fromTextFile(paths: _*)
  def fromTextFile(paths: List[String]) = TextInput.fromTextFile(paths)
  def fromDelimitedTextFile[A : Manifest : WireFormat]
      (path: String, sep: String = "\t")
      (extractFn: PartialFunction[List[String], A]) = TextInput.fromDelimitedTextFile(path, sep)(extractFn)
  def toTextFile[A : Manifest](dl: DList[A], path: String, overwrite: Boolean = false) = TextOutput.toTextFile(dl, path, overwrite)
  def toDelimitedTextFile[A <: Product : Manifest](dl: DList[A], path: String, sep: String = "\t", overwrite:Boolean = false) = TextOutput.toDelimitedTextFile(dl, path, sep, overwrite)


  // Join lib

  def join[K : Manifest : WireFormat : Grouping,
           A : Manifest : WireFormat,
           B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      = Join.join(d1, d2)


  def joinRight[K : Manifest : WireFormat : Grouping,
                A : Manifest : WireFormat,
                B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)], default: (K, B) => A)
      = Join.joinRight(d1, d2, default)


  def joinRight[K : Manifest : WireFormat : Grouping,
                A : Manifest : WireFormat,
                B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      = Join.joinRight(d1, d2)


  def joinLeft[K : Manifest : WireFormat : Grouping,
               A : Manifest : WireFormat,
               B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)], default: (K, A) => B)
      = Join.joinLeft(d1, d2, default)


  def joinLeft[K : Manifest : WireFormat : Grouping,
               A : Manifest : WireFormat,
               B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      = Join.joinLeft(d2, d1)

  def coGroup[K  : Manifest : WireFormat : Grouping,
              A : Manifest : WireFormat,
              B : Manifest : WireFormat]
      (d1: DList[(K, A)], d2: DList[(K, B)])
      = Join.coGroup(d1, d2)


  // Avro
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: String*) = AvroInput.fromAvroFile(paths: _*)
  def fromAvroFile[A : Manifest : WireFormat : AvroSchema](paths: List[String]) = AvroInput.fromAvroFile(paths)
  def toAvroFile[B : AvroSchema](dl: DList[B], path: String) = AvroOutput.toAvroFile(dl, path)

}
