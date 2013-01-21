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

import org.apache.hadoop.mapreduce.Partitioner
import core._
import impl.ScoobiConfiguration

/** Custom partitioner for tagged key-values. */
trait TaggedPartitioner extends Partitioner[TaggedKey, TaggedValue]

/** Companion object for dynamically constructing a subclass of TaggedPartitioner. */
object TaggedPartitioner {
  def apply(name: String, tags: Map[Int, (WireReaderWriter, KeyGrouping)])(implicit sc: ScoobiConfiguration): RuntimeClass =
    MetadataClassBuilder[MetadataTaggedPartitioner](name, tags).toRuntimeClass
}

/**
 * This partitioner uses the grouping of the current key tag and partitions based on the key value
 */
abstract class MetadataTaggedPartitioner extends TaggedPartitioner with MetadataWireFormats with MetadataGroupings {
  def getPartition(key: TaggedKey, value: TaggedValue, numPartitions: Int): Int = {
    grouping(key.tag).partition(key.get(key.tag), numPartitions)
  }
}

