package com.nicta.scoobi.io.sequence
import com.nicta.scoobi.DList
import com.nicta.scoobi.impl.plan.Smart
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.InputStore
import com.nicta.scoobi.impl.plan.AST
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.io.text.TextInput.SimplerTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.io.NullWritable
import com.nicta.scoobi.io.text.TextInput.StringWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader
import com.nicta.scoobi.WireFormat

object SequenceInput {
  /** Create a distributed list from a file. */
  def fromSequenceFile[T: Manifest: WireFormat](path: String): DList[T] = new DList(Smart.Load(new SequenceLoader[T](path)))

  /**
   * A Loader that will load the input from a specified path using SimplerTextInputFormat, a
   * wrapper around Hadoop's TextInputFormat.
   */
  class SequenceLoader[T: Manifest: WireFormat](path: String) extends Loader[T] {
    def mkInputStore(node: AST.Load[T]) = new InputStore(node) {
      def inputTypeName = typeName
      val inputPath = new Path(path)
      val inputFormat = classOf[SimplerSequenceFileInputFormat]
    }
  }

  /**
   * A wrapper around SequenceFileInputFormat that changes the type parameterisation
   * from Text-Text to NullWritable-StringWritable. This input
   * format is then used as the basis for loading files into a Scoobi job.
   */
  class SimplerSequenceFileInputFormat extends SequenceFileInputFormat[NullWritable, StringWritable] {

    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, StringWritable] = {

      context.setStatus(split.toString)
      val rr = new SimplerSequenceFileRecordReader
      rr.initialize(split.asInstanceOf[FileSplit], context)
      rr
    }
  }
  /** A wrapper around LineRecordReader. */
  private class SimplerSequenceFileRecordReader extends SequenceFileRecordReader[NullWritable, StringWritable] {
    private val lrr = new SequenceFileRecordReader
    private val value = new StringWritable("")

    override def close() = lrr.close()

    override def getCurrentKey: NullWritable = NullWritable.get

    override def getCurrentValue = { value.set(lrr.getCurrentValue.toString); println("read: " + value.get); value }

    override def getProgress: Float = lrr.getProgress

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = lrr.initialize(split, context)

    override def nextKeyValue: Boolean = lrr.nextKeyValue

    def createKey: NullWritable = NullWritable.get
  }

}