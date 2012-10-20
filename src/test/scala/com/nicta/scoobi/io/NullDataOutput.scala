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
package io

import java.io.DataOutput

object NullDataOutput extends DataOutput {
  def write(b: Int) {}
  def write(b: Array[Byte]) {}
  def write(b: Array[Byte], off: Int, len: Int) {}
  def writeBoolean(v: Boolean) {}
  def writeByte(v: Int) {}
  def writeShort(v: Int) {}
  def writeChar(v: Int) {}
  def writeInt(v: Int) {}
  def writeLong(v: Long) {}
  def writeFloat(v: Float) {}
  def writeDouble(v: Double) {}
  def writeBytes(s: String) {}
  def writeChars(s: String) {}
  def writeUTF(s: String) {}
}