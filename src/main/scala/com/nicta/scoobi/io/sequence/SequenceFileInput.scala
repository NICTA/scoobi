package com.nicta.scoobi.io.sequence

import org.apache.hadoop.io.Writable
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.DList
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.impl.plan.AST
import com.nicta.scoobi.impl.plan.Smart
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.RecordReader
import java.lang.reflect.ParameterizedType
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import java.io.DataOutput
import java.io.DataInput
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/** 
 * Reader for sequence files. It uses hadoop's types instead of scoobi's types and returns the key-values contained in the 
 * sequence file as pairs.
 */
object SequenceFileInput {

  /**
   * Reading in from a sequence file:
   *   - specify path to sequence file
   *   - need to specify the Writable/WritableComparable classes that have been serialised in the sequence file
   *   - provide functions that take can get the value out of Writables plus the WireFormat definitions of K and V; this
   *     is all implicit so that for a lot of the common cases you don't have to fill it in
   */
  def fromSequenceFile[K <: Writable: Manifest, V <: Writable: Manifest](path: String): DList[(K, V)] = {
    val loader = new Loader[(K, V)] {
      def mkInputStore(node: AST.Load[(K, V)]) = new InputStore(node) {
        def inputTypeName = typeName
        val inputPath = new Path(path)
        val inputFormat = classOf[SimplerSequenceFileInputFormat]
      }
    }
    new DList(Smart.Load(loader))
  }
}

class SimplerSequenceFileInputFormat extends SequenceFileInputFormat[NullWritable, ScoobiWritable[(Writable, Writable)]] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, ScoobiWritable[(Writable, Writable)]] = {
    context.setStatus(split.toString)
    val rr = new SimplerSequenceFileRecordReader
    rr.initialize(split.asInstanceOf[FileSplit], context)
    rr
  }
}

/**
 * A wrapper around a pair of hadoop's Writables  to make it a ScoobiWritable type. This class is purely to fool
 * scoobi to accept a tuple of writables, its implementation is never actually called, so the methods are only dummies.
 */
class WritablePair(val x: Writable, val y: Writable) extends ScoobiWritable[(Writable, Writable)]((x, y)) {
  def write(out: DataOutput) = {
    throw new NotImplementedException
  }
  def readFields(in: DataInput) = {
    throw new NotImplementedException
  }
}

class SimplerSequenceFileRecordReader[K <: Writable: Manifest, V <: Writable: Manifest] extends SequenceFileRecordReader[NullWritable, ScoobiWritable[(Writable, Writable)]] {
  private val lrr = new SequenceFileRecordReader[K, V]

  override def close() = lrr.close()

  override def getCurrentKey: NullWritable = NullWritable.get

  override def getCurrentValue = { new WritablePair(lrr.getCurrentKey, lrr.getCurrentValue) }

  override def getProgress: Float = lrr.getProgress

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = lrr.initialize(split, context)

  override def nextKeyValue: Boolean = lrr.nextKeyValue

  def createKey: NullWritable = NullWritable.get
}