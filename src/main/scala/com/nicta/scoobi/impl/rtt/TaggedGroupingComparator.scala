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

import org.apache.hadoop.io.{DataInputBuffer, RawComparator}
import core._
import org.apache.hadoop.conf.Configuration

/** Custom GroupingComparator for tagged keys. */
trait TaggedGroupingComparator extends RawComparator[TaggedKey]

/** Companion object for dynamically constructing a subclass of TaggedGroupingComparator. */
object TaggedGroupingComparator {
  def apply(name: String, tags: Map[Int, (WireReaderWriter, KeyGrouping)], classLoader: ClassLoader, configuration: Configuration) : RuntimeClass =
    MetadataClassBuilder[MetadataTaggedGroupingComparator](name, tags, classLoader, configuration).toRuntimeClass
}

abstract class MetadataTaggedGroupingComparator extends TaggedGroupingComparator with MetadataWireFormats with MetadataGroupings {
  val (buffer1, buffer2) = (new DataInputBuffer, new DataInputBuffer)

  /**
   * implementation of the compare method, using the keys tags
   * If the tags are different we compare the tags. If they are the same we using the grouping instance for the
   * current tag
   */
  def compare(key1: TaggedKey, key2: TaggedKey) =
    if (key1.tag == key2.tag) grouping(key1.tag).groupCompare(key1.get(key1.tag), key2.get(key1.tag)).toInt
    else                      key1.tag - key2.tag

  /**
   * implementation of the compare method, with buffers
   */
  def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int) = {
    buffer1.reset(b1, s1, l1)
    buffer2.reset(b2, s2, l2)

    // if there's only one tag, it is not written to the input stream
    val (tag1, tag2) = if (tags.size == 1) (tags(0), tags(0)) else (buffer1.readInt, buffer2.readInt)
    if (tag1 == tag2) grouping(tag1).groupCompare(wireFormat(tag1).fromWire(buffer1), wireFormat(tag1).fromWire(buffer2)).toInt
    else              tag1 - tag2
  }
}
