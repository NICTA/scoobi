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

import scala.collection.mutable
import core.WireFormat
import java.io.{DataInput, DataOutput}

/** A collection of types indexed by a tag */
trait Tagged extends Configured { self =>
  private var t: Int = 0
  def tag = t
  def setTag(tag1: Int) { self.t = tag1 }

  /* Get/set the value for a given tag */
  def get(tag: Int): Any
  def set(x: Any)
}

/**
 * WireFormats accessible by tags
 */
trait MetadataWireFormats extends TaggedMetadata {
  def wireFormat(tag: Int): WireFormat[Any] = metaDatas(tag).productElement(0).asInstanceOf[WireFormat[Any]]
}

/**
 * This trait represents a Writable capable of reading/writing different type of data depending on a tag
 * and having WireFormats accessible for each tag
 */
trait MetadataTaggedWritable extends MetadataWireFormats {

  def tag: Int
  def setTag(t: Int)

  private val values = new mutable.HashMap[Int, Any]
  def get(t: Int) = values(t)

  def set(value: Any) {
    values(tag) = value
  }

  def setValue(value: =>Any) {
    values(tag) = value
  }

  def write(out: DataOutput) {
    // if there are more than one tag, write the tag to the output stream
    if (tags.size > 1) out.writeInt(tag)
    wireFormat(tag).toWire(get(tag), out)
  }

  def readFields(in: DataInput) {
    // if there are more than one tag, read the tag from the input stream
    setTag(if (tags.size > 1) in.readInt else tags(0))
    set(wireFormat(tag).fromWire(in))
  }

}
