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
package com.nicta.scoobi.impl.rtt

import org.apache.hadoop.mapreduce.Partitioner


/** Custom partitioner for tagged key-values. */
class TaggedPartitioner extends Partitioner[TaggedKey, TaggedValue] {

  /** Key-values are tagged with the output channel they are destined for. It is vital
    * that a given reducer task only receives key-values from a single channel. To ensure
    * this, partition key-values by the key's tag. */
  def getPartition(key: TaggedKey, value: TaggedValue, numPartitions: Int): Int =
    (key.tag.hashCode() & Int.MaxValue) % numPartitions
}
