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

import org.apache.hadoop.io.Writable
import core._
import impl.ScoobiConfiguration

/**
 * A tagged value for Hadoop values. Specifically this will be a V2 type so must
 * implement the Writable interface.
 *
 * Before using a TaggedValue the appropriate tag must be set. By default, it is 0
 */
trait TaggedValue extends Tagged with Writable

/** Companion object for dynamically constructing a subclass of TaggedValue. */
object TaggedValue {
  def apply(name: String, tags: Map[Int, Tuple1[WireReaderWriter]])(implicit sc: ScoobiConfiguration): RuntimeClass =
    MetadataClassBuilder[MetadataTaggedValue](name, tags).toRuntimeClass
}

abstract class MetadataTaggedValue extends TaggedValue with MetadataTaggedWritable
