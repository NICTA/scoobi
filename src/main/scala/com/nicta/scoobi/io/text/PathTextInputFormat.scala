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
package com.nicta
package scoobi
package io
package text

import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, RecordReader, TaskAttemptContext, InputSplit}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.LineReader
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.commons.logging.LogFactory


class PathTextInputFormat extends FileInputFormat[Text, Text]  {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, Text] = {
    val delimiter = context.getConfiguration.get("textinputformat.record.delimiter")
    var recordDelimiterBytes: Array[Byte] = null
    if (null != delimiter) recordDelimiterBytes = delimiter.getBytes
    new PathLineRecordReader(recordDelimiterBytes)
  }

  override def isSplitable(context: JobContext, file: Path) =
    new CompressionCodecFactory(context.getConfiguration).getCodec(file) == null
}

/**
 * Treats keys as the file name and value as line.
 */
class PathLineRecordReader(recordDelimiter: Array[Byte]) extends RecordReader[Text, Text] {
  private val LOG = LogFactory.getLog(classOf[PathLineRecordReader].getName)

  private var compressionCodecs: CompressionCodecFactory = null
  private var start = 0L
  private var pos = 0L
  private var end = 0L
  private var in: LineReader = _
  private var maxLineLength: Int = 0
  private var key: Text = new Text()
  private var value: Text = new Text()

  def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    val configuration = context.getConfiguration
    val split = inputSplit.asInstanceOf[FileSplit]
    key = new Text(split.getPath.toUri.getPath)
    value = new Text

    maxLineLength = configuration.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE)
    start = split.getStart
    end = start + split.getLength
    val file = split.getPath
    compressionCodecs = new CompressionCodecFactory(configuration)
    val codec = compressionCodecs.getCodec(file)

    // open the file and seek to the start of the split
    val fs = file.getFileSystem(configuration)
    val fileIn = fs.open(split.getPath)
    var skipFirstLine = false
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), configuration, recordDelimiter)
      end = Long.MaxValue
    } else {
      if (start != 0) {
        skipFirstLine = true
        start -= 1
        fileIn.seek(start)
      }
      in = new LineReader(fileIn, configuration, recordDelimiter)
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text, 0, scala.math.min(Int.MaxValue, end - start).toInt)
    }
    pos = start
  }

  def getCurrentKey = key
  def getCurrentValue = value
  def nextKeyValue = next(key, value)

  private def next(key: Text, value: Text): Boolean = synchronized {
    while (pos < end) {
      val newSize: Int = in.readLine(value, maxLineLength, scala.math.max(scala.math.min(Int.MaxValue, end - pos), maxLineLength).toInt)
      if (newSize == 0) {
        return false
      }
      pos += newSize
      if (newSize < maxLineLength) {
        return true
      }
      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize))
    }
    false
  }

  /**
   * Get the progress within the split
   */
  def getProgress = {
    if (start == end) 0.0f
    else              Math.min(1.0f, (pos - start) / (end - start))
  }

  def getPos = synchronized { pos }

  def close = synchronized {
    if (in != null) in.close
  }
}

/**




 */
