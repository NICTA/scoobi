package com.nicta.scoobi
package core

import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType

/** The container for persisting a DList. */
case class DListPersister[A](dlist: DList[A], sink: DataSink[_, _, A]) {
  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(sink = sink.outputCompression(codec))
}
