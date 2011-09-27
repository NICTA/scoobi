/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.LineRecordReader
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter


/** A wrapper around TextInputFormat that changes the type paramerterisation
  * from LongWritable-Text to NullWritable-StringScoobiWritable. This input
  * format is then used as the basis for loading files into a Scoobi job. */
class SimplerTextInputFormat
  extends FileInputFormat[NullWritable, StringScoobiWritable] {

  def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter):
    RecordReader[NullWritable, StringScoobiWritable] = {

    reporter.setStatus(split.toString)
    new SimplerLineRecordReader(job, split.asInstanceOf[FileSplit])
  }
}


/** A wrapper around Text to make it a ScoobiWritable type. */
class StringScoobiWritable(x: String) extends ScoobiWritable[String](x) {
  def write(out: DataOutput) = (new Text(get)).write(out)
  def readFields(in: DataInput) = { val y = new Text; y.readFields(in); set(y.toString) }
}


/** A wrapper around LineRecordReader. */
class SimplerLineRecordReader(job: JobConf, fileSplit: FileSplit)
  extends RecordReader[NullWritable, StringScoobiWritable] {

  val lrr = new LineRecordReader(job, fileSplit)

  def close() = lrr.close()

  def createKey: NullWritable = NullWritable.get

  def createValue: StringScoobiWritable = {
    new StringScoobiWritable("")
  }

  def getPos: Long = lrr.getPos

  def getProgress: Float = lrr.getProgress

  def next(key: NullWritable, value: StringScoobiWritable) = {
    var k: LongWritable = new LongWritable
    var v: Text = new Text
    val more = lrr.next(k, v)
    value.set(v.toString)
    more
  }
}
