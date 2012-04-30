package com.nicta.scoobi.io

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