package com.nicta.scoobi
package impl
package io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.avro.generic.GenericRecord
import com.nicta.scoobi.impl.util.Compatibility
import scala.collection.JavaConversions._
import org.apache.hadoop.io.{Writable, SequenceFile, NullWritable}
import com.nicta.scoobi.impl.rtt.ScoobiWritable
import com.nicta.scoobi.core.WireFormat
import com.nicta.scoobi.io.sequence.SeqSchema

/**
 * This Iterator iterates over values found in all the files corresponding to a given path, provided that there is a way to
 * get individual iterators to each file
 */
class GlobIterator[A](path: Path, iterator: Path => CloseableIterator[A])(implicit configuration: Configuration) extends Iterator[A] {

  private lazy val fs = Files.fileSystem(path)

  private var allInitialised = false
  private var remainingReaders: Stream[CloseableIterator[A]] = Stream()

  def init {
    if (!allInitialised)  {
      remainingReaders = fs.globStatus(path).toStream.map(status => iterator(status.getPath))
      allInitialised = true
    }
  }

  def next(): A = {
    init
    remainingReaders match {
      case cur #:: rest =>
        val n = cur.next()
        if (!cur.hasNext) moveNextReader()
        n
    }
  }

  def hasNext: Boolean = {
    init
    remainingReaders match {
      case Stream.Empty         => false
      case cur #:: Stream.Empty => cur.hasNext
      case cur #:: rest         => cur.hasNext || { moveNextReader(); hasNext }
    }
  }

  private def moveNextReader() {
    remainingReaders match {
      case cur #:: rest => cur.close(); remainingReaders = rest
      case _            =>
    }
  }

  def close {
    Option(remainingReaders).map(rs => rs.foreach(_.close))
  }
}

object GlobIterator {
  /** @return an iterator on a scala.io.Source */
  def sourceIterator(implicit configuration: Configuration) = (path: Path) => {
    val fs = path.getFileSystem(configuration)

    new CloseableIterator[String] {
      lazy val is = scala.io.Source.fromInputStream(fs.open(path))
      lazy val iterator = is.getLines
      def close() = is.close()
    }
  }

  /** @return an iterator on an Avro FileReader */
  def avroIterator(implicit configuration: Configuration) = (path: Path) => {
    import org.apache.avro.mapred.FsInput
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.generic.GenericDatumReader

    new CloseableIterator[GenericRecord] {
      private val in = new FsInput(path, configuration)
      private val reader = new GenericDatumReader[GenericRecord]()
      lazy val iterator: Iterator[GenericRecord] = DataFileReader.openReader(in, reader).iterator

      def close() = in.close
    }
  }

  /** iterator for Scoobi writables */
  def scoobiWritableIterator[A](value: ScoobiWritable[A])(implicit configuration: Configuration) = (path: Path) => {
    val reader = Compatibility.newSequenceFileReader(configuration, path)
    val key = NullWritable.get

    new CloseableIterator[A] {
      lazy val iterator = new Iterator[A] {
        private var empty = false
        private var initialised = false

        def next() = {
          init
          try     value.get
          finally empty = !readNext()
        }

        def hasNext: Boolean = {
          init
          !empty
        }

        private def init {
          if (!initialised) {
            empty = !readNext()
            value.configuration = configuration
            initialised = true
          }
        }

        private def readNext(): Boolean = {
          try reader.next(key, value)
          catch { case e: Throwable => e.printStackTrace; false }
        }
      }
      def close() = reader.close()
    }
  }

  /** iterator for key sequences */
  def keySequenceIterator[K](implicit configuration: Configuration, wf: WireFormat[K], schema: SeqSchema[K]) = (path: Path) => try {
    val reader = Compatibility.newSequenceFileReader(configuration, path)
    new SequenceCloseableIterator[K](reader)((key: Writable, value: Writable) => schema.fromWritable(key.asInstanceOf[schema.SeqType]))
  } catch { case e: Exception => emptyCloseableIterator[K] }

  /** iterator for value sequences */
  def valueSequenceIterator[V](implicit configuration: Configuration, wf: WireFormat[V], schema: SeqSchema[V]) = (path: Path) => try {
    val reader = Compatibility.newSequenceFileReader(configuration, path)
    new SequenceCloseableIterator[V](reader)((key: Writable, value: Writable) => schema.fromWritable(value.asInstanceOf[schema.SeqType]))
  } catch { case e: Exception => emptyCloseableIterator[V] }

  /** iterator for sequences */
  def sequenceIterator[K, V](implicit configuration: Configuration, wfk: WireFormat[K], schemaK: SeqSchema[K], wfv: WireFormat[V], schemaV: SeqSchema[V]) = (path: Path) => try {
    val reader = Compatibility.newSequenceFileReader(configuration, path)
    new SequenceCloseableIterator[(K, V)](reader)((key: Writable, value: Writable) => (schemaK.fromWritable(key.asInstanceOf[schemaK.SeqType]), schemaV.fromWritable(value.asInstanceOf[schemaV.SeqType])))
  } catch { case e: Exception => emptyCloseableIterator[(K, V)] }

  class SequenceCloseableIterator[A](reader: SequenceFile.Reader)(f: (Writable, Writable) => A) extends CloseableIterator[A] {
    val key = reader.getKeyClass.newInstance.asInstanceOf[Writable]
    val value = reader.getValueClass.newInstance.asInstanceOf[Writable]

    lazy val iterator = new Iterator[A] {
      private var empty = false
      private var initialised = false

      def next() = {
        init
        try     f(key, value)
        finally empty = !readNext()
      }

      def hasNext: Boolean = {
        init
        !empty
      }

      private def init {
        if (!initialised) {
          empty = !readNext()
          initialised = true
        }
      }

      private def readNext(): Boolean = {
        try     reader.next(key, value)
        catch { case e: Throwable => e.printStackTrace; false }
      }
    }
    def close() = reader.close()
  }

  def emptyCloseableIterator[A] = new CloseableIterator[A] {
    lazy val iterator = new Iterator[A] {
      def next() = ???
      def hasNext: Boolean = false
    }
    def close() = ()
  }
}

/**
 * encapsulation of an Iterator with a close method to clean up resources
 */
trait CloseableIterator[A] extends Iterator[A] {
  def iterator: Iterator[A]
  private lazy val closeableIterator = iterator

  def hasNext = closeableIterator.hasNext
  def next()  = closeableIterator.next

  def close(): Unit
}
