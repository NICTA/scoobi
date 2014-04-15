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
package sequence

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}

import core._
import partition._
import impl.ScoobiConfiguration._
import impl.io.Files
import org.apache.hadoop.conf.Configuration
import impl.ScoobiConfigurationImpl
import core.ScoobiConfiguration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.SequenceFile.CompressionType

/** Smart functions for persisting distributed lists by storing them as Sequence files. */
trait SequenceOutput {

  def keySchemaSequenceFile[K](path: String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convK: SeqSchema[K]) = {

    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = classOf[NullWritable]

    val converter = new InputOutputConverter[convK.SeqType, NullWritable, K] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: NullWritable) = convK.fromWritable(k)
      def toKeyValue(kv: K)(implicit configuration: Configuration) = kv match {
        case (k1, _) => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
        case k1      => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
      }
    }
    new SeqSink[convK.SeqType, NullWritable, K](path, keyClass, valueClass, converter, overwrite, check)
  }

  def valueSchemaSequenceFile[V](path: String, overwrite: Boolean = false,
                                 check: Sink.OutputCheck = Sink.defaultOutputCheck,
                                 checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit convV: SeqSchema[V], sc: ScoobiConfiguration) = {
    val keyClass = classOf[NullWritable]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[NullWritable, convV.SeqType, V] {
      def fromKeyValue(context: InputContext, k: NullWritable, v: convV.SeqType) = convV.fromWritable(v)
      def toKeyValue(kv: V)(implicit configuration: Configuration) = kv match {
        case (_,v) => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
        case v     => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
      }
    }
    new SeqSink[NullWritable, convV.SeqType, V](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

  def schemaSequenceSink[K, V](path: String, overwrite: Boolean = false,
                               check: Sink.OutputCheck = Sink.defaultOutputCheck,
                               checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(implicit convK: SeqSchema[K], convV: SeqSchema[V], sc: ScoobiConfiguration)= {

    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: convV.SeqType) = (convK.fromWritable(k), convV.fromWritable(v))
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }
    new SeqSink[convK.SeqType, convV.SeqType, (K, V)](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

  def sequenceSink[K <: Writable, V <: Writable](path: String, overwrite: Boolean = false,
                                                 check: Sink.OutputCheck = Sink.defaultOutputCheck,
                                                 checkpoint: Boolean = false, expiryPolicy: ExpiryPolicy = ExpiryPolicy.default)(
      implicit mk: Manifest[K], mv: Manifest[V], sc: ScoobiConfiguration) = {
    val keyClass = mk.runtimeClass.asInstanceOf[Class[K]]
    val valueClass = mv.runtimeClass.asInstanceOf[Class[V]]

    val converter = new InputOutputConverter[K, V, (K, V)] {
      def fromKeyValue(context: InputContext, k: K, v: V) = (k, v)
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (kv._1, kv._2)
    }
    new SeqSink[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite, check, Checkpoint.create(Some(path), expiryPolicy, checkpoint))
  }

  /**
   * Partitioned sinks
   */
  def keySchemaPartitionedSequenceSink[P, K](path: String, partition: P => String, overwrite: Boolean = false,
                                             check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convK: SeqSchema[K], sc: ScoobiConfiguration) = {
    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = classOf[NullWritable]

    val converter = new InputOutputConverter[convK.SeqType, NullWritable, K] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: NullWritable) = convK.fromWritable(k)
      def toKeyValue(kv: K)(implicit configuration: Configuration) = kv match {
        case (k1, _) => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
        case k1      => (convK.toWritable(k1.asInstanceOf[K]), NullWritable.get)
      }
    }
    val subsink = new SeqSink[convK.SeqType, NullWritable, K](path, keyClass, valueClass, converter, overwrite, check)
    new PartitionedSink(subsink, classOf[PartitionedSequenceOutputFormat[P, convK.SeqType, NullWritable]], path, partition, overwrite, check)
  }

  def valueSchemaPartitionedSequenceSink[P, V](path: String, partition: P => String, overwrite: Boolean = false,
                                               check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convV: SeqSchema[V], sc: ScoobiConfiguration) = {
    val keyClass = classOf[NullWritable]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[NullWritable, convV.SeqType, V] {
      def fromKeyValue(context: InputContext, k: NullWritable, v: convV.SeqType) = convV.fromWritable(v)
      def toKeyValue(kv: V)(implicit configuration: Configuration) = kv match {
        case (_,v) => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
        case v     => (NullWritable.get, convV.toWritable(v.asInstanceOf[V]))
      }
    }
    val subsink = new SeqSink[NullWritable, convV.SeqType, V](path, keyClass, valueClass, converter, overwrite, check)
    new PartitionedSink(subsink, classOf[PartitionedSequenceOutputFormat[P, NullWritable, convV.SeqType]], path, partition, overwrite, check)
  }

  def schemaPartitionedSequenceSink[P, K, V](path: String, partition: P => String, overwrite: Boolean = false,
                                             check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit convK: SeqSchema[K], convV: SeqSchema[V], sc: ScoobiConfiguration) = {
    val keyClass = convK.mf.runtimeClass.asInstanceOf[Class[convK.SeqType]]
    val valueClass = convV.mf.runtimeClass.asInstanceOf[Class[convV.SeqType]]

    val converter = new InputOutputConverter[convK.SeqType, convV.SeqType, (K, V)] {
      def fromKeyValue(context: InputContext, k: convK.SeqType, v: convV.SeqType) = (convK.fromWritable(k), convV.fromWritable(v))
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (convK.toWritable(kv._1), convV.toWritable(kv._2))
    }
    val subsink = new SeqSink[convK.SeqType, convV.SeqType, (K, V)](path, keyClass, valueClass, converter, overwrite, check)
    new PartitionedSink(subsink, classOf[PartitionedSequenceOutputFormat[P, convK.SeqType, convV.SeqType]], path, partition, overwrite, check)
  }

  def partitionedSequenceSink[P, K <: Writable, V <: Writable](path: String, partition: P => String, overwrite: Boolean = false,
                                                 check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit mk: Manifest[K], mv: Manifest[V], sc: ScoobiConfiguration) = {
    val keyClass = mk.runtimeClass.asInstanceOf[Class[K]]
    val valueClass = mv.runtimeClass.asInstanceOf[Class[V]]

    val converter = new InputOutputConverter[K, V, (K, V)] {
      def fromKeyValue(context: InputContext, k: K, v: V) = (k, v)
      def toKeyValue(kv: (K, V))(implicit configuration: Configuration) = (kv._1, kv._2)
    }
    val subsink = new SeqSink[K, V, (K, V)](path, keyClass, valueClass, converter, overwrite, check)
    new PartitionedSink(subsink, classOf[PartitionedSequenceOutputFormat[P, K, V]], path, partition, overwrite, check)
  }
}

object SequenceOutput extends SequenceOutput

/** class that abstracts all the common functionality of persisting to sequence files. */
case class SeqSink[K, V, B](path: String,
                            keyClass: Class[K],
                            valueClass: Class[V],
                            outputConverter: InputOutputConverter[K, V, B],
                            overwrite: Boolean,
                            check: Sink.OutputCheck = Sink.defaultOutputCheck,
                            checkpoint: Option[Checkpoint] = None,
                            compression: Option[Compression] = None) extends DataSink[K, V, B] with SinkSource {

  private lazy val logger = LogFactory.getLog("scoobi.SequenceOutput")

  protected val output = new Path(path)

  def toSource = new SeqSource(Seq(path), SequenceInput.defaultSequenceInputFormat, outputConverter)

  def outputFormat(implicit sc: ScoobiConfiguration) = classOf[SequenceFileOutputFormat[K, V]]
  def outputKeyClass(implicit sc: ScoobiConfiguration) = keyClass
  def outputValueClass(implicit sc: ScoobiConfiguration) = valueClass

  def outputCheck(implicit sc: ScoobiConfiguration) {
    check(output, overwrite, sc)
  }
  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)

  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {}

  override def outputSetup(implicit sc: ScoobiConfiguration) {
    super.outputSetup(sc)
    if (Files.pathExists(output)(sc.configuration) && overwrite) {
      logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
      Files.deletePath(output)(sc.configuration)
    }
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = getClass.getSimpleName+": "+outputPath(new ScoobiConfigurationImpl).getOrElse("none")
}

/**
 * This format creates a new sequence record writer for each different path that's generated by the partition function
 * Each record writer defines a specific OutputCommitter that will define a different work directory for a given key.
 *
 * All the generated paths will be created under temporary dir/sink id in order to collect them
 * more rapidly with just a rename of directories (see OutputChannel)
 */
class PartitionedSequenceOutputFormat[P, K, V] extends PartitionedOutputFormat[P, K, V] {

  protected def getBaseRecordWriter(context: TaskAttemptContext, path: Path): RecordWriter[K, V] =
    new SequenceFileOutputFormat[K, V] {
      override def getOutputCommitter(context: TaskAttemptContext) = new FileOutputCommitter(path, context) {
        // we need to use path as the work path for the record writers because it
        // already contains the work directories
        override def getWorkPath = path
      }
      /**
       * override this method to give an output name without a directory
       * so that files are written directly as year=2014/month=01/day=23/out-xxxx instead of
       * year=2014/month=01/day=23/ch3-4/out-xxxx
       *
       * Because ch3-4/out is the default basename configured in ChannelOutputFormat
       */
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val committer: FileOutputCommitter = getOutputCommitter(context).asInstanceOf[FileOutputCommitter]
        new Path(committer.getWorkPath, FileOutputFormat.getUniqueFile(context, "out", extension))
      }

    }.getRecordWriter(context)
}
