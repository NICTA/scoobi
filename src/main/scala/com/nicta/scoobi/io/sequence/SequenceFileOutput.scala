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
package com.nicta.scoobi.io.sequence

import org.apache.hadoop.fs.Path
import com.nicta.scoobi.DList
import com.nicta.scoobi.DListPersister
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.impl.plan.AST
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util._
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.nicta.scoobi.impl.rtt.TaggedValue
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

/** Smart functions for persisting distributed lists by storing them as sequence files. */
object SequenceFileOutput {

  /**
   * Specify a distributed list to be persistent by storing it to disk as a
   * sequence file.
   */
  def toSequenceFile[A: WireFormat](dl: DList[A], path: String): DListPersister[A] = {
    new DListPersister(dl, new SequenceFilePersister(path))
  }

  /** A Persister that will store the output to a specified path using Hadoop's SequenceFileOutputFormat. */
  class SequenceFilePersister[A](path: String) extends Persister[A] {
    def mkOutputStore(node: AST.Node[A]) = new OutputStore(node) {
      def outputTypeName = typeName
      val outputPath = new Path(path)
      val outputFormat = classOf[MySequenceFileOutputFormat[_, _]]
    }
  }
}

class MySequenceFileOutputFormat[K, V] extends SequenceFileOutputFormat[K, V] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[K, V] = {
    val conf: Configuration = context.getConfiguration()

    var codec: CompressionCodec = null
    var compressionType: CompressionType = CompressionType.NONE
    if (FileOutputFormat.getCompressOutput(context)) {
      // find the kind of compression to do
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context)

      // find the right codec
      val codecClass = FileOutputFormat.getOutputCompressorClass(context, classOf[DefaultCodec])
      codec = ReflectionUtils.newInstance(codecClass, conf).asInstanceOf[CompressionCodec]
    }
    // get the path of the temporary output file 
    val file: Path = getDefaultWorkFile(context, "");
    val fs: FileSystem = file.getFileSystem(conf);

    val out: SequenceFile.Writer =
      SequenceFile.createWriter(fs, conf, file,
        context.getOutputKeyClass(),
        context.getOutputValueClass(),
        compressionType,
        codec,
        context)

    return new RecordWriter[K, V]() {
      def write(key: K, value: V) {
        // here is my problem: value is of type V1, and I'd like to get to 
        // the underlying Tuple2, but there is no accessor. 
        // my plan is to get the _1 element as a key and _2 as a value for the 
        // sequence file , but I have no way to get to the tuple.
        //val pair = value.asInstanceOf[(Writable, Writable)]
        println(key, key.getClass, value, value.getClass)
        out.append(key, value);
      }

      def close(reporter: TaskAttemptContext) { out.close() }
    }
  }
}


