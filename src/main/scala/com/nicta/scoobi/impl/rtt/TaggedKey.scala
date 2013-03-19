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
package impl
package rtt

import org.apache.hadoop.io.WritableComparable
import core._
import impl.ScoobiConfigurationImpl._

/**
 * A tagged value for Hadoop keys. Specifically this will be a K2 type so must
 * implement the WritableComparable interface
 */
trait TaggedKey extends Tagged with WritableComparable[TaggedKey]

/** Companion object for dynamically constructing a subclass of TaggedKey. */
object TaggedKey {
  def apply(name: String, tags: Map[Int, (WireReaderWriter, KeyGrouping)])(implicit sc: ScoobiConfiguration): RuntimeClass =
    MetadataClassBuilder[MetadataTaggedKey](name, tags).toRuntimeClass
}

/**
 * Set of Groupings accessible by tag
 */
trait MetadataGroupings extends TaggedMetadata {
  def grouping(tag: Int): Grouping[Any] = metaDatas(tag).productElement(1).asInstanceOf[Grouping[Any]]
}

/**
 * Tagged key with some metadata describing each channel. It has WireFormats and Groupings
 */
abstract class MetadataTaggedKey extends TaggedKey with MetadataTaggedWritable with MetadataGroupings {
  def compareTo(other: TaggedKey): Int = other match {
    case tk: MetadataTaggedKey if tk.tag == tag => grouping(tag).sortCompare(get(tag), tk.get(tag)).toInt
    case tk: MetadataTaggedKey                  => tag - tk.tag
    case _                                      => 0
  }
}

