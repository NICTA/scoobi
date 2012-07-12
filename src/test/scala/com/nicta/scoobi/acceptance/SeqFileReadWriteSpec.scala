package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import testing.TestFiles
import org.apache.hadoop.io._
import org.specs2.matcher.MatchResult

import java.io.IOException

class SeqFileReadWriteSpec extends NictaSimpleJobs {

  implicit def TextOrdering = new Ordering[Text] {
    def compare(x: Text, y: Text): Int = x.compareTo(y)
  }

  implicit def BytesOrdering = new Ordering[BytesWritable] {
    def compare(x: BytesWritable, y: BytesWritable): Int = x.compareTo(y)
  }

  implicit def DoubleOrdering = new Ordering[DoubleWritable] {
    def compare(x: DoubleWritable, y: DoubleWritable): Int = x.compareTo(y)
  }

  "Reading Text -> Text Sequence file" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", "b"), ("c", "d"), ("e", "f")))

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, Text](tmpSeqFile)
    persist(loadedTestData.materialize).toSeq.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"), ("e", "f"))
  }

  "Writing Text -> Text Sequence file" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // can not create a new DList containing Text because of issue # 99
    val testDList: DList[(String, String)] = DList(("a", "b"), ("c", "d"))
    persist(toSequenceFile(testDList.map((kv: (String, String)) => (new Text(kv._1), new Text(kv._2))), filePath, true))

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, Text](filePath)
    persist(loadedTestData.materialize).toSeq.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"))
  }

  "Reading Text -> IntWritable, Writing BytesWritable -> DoubleWritable" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", 1), ("b", 2)))
    val outPath = createTempFile("iotest.out")

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, IntWritable](tmpSeqFile)
    val mappedTestData = loadedTestData.map(x => (new BytesWritable(x._1.getBytes), new DoubleWritable(x._2.get)))
    persist(toSequenceFile(mappedTestData, outPath, true))

    // load data to check it was stored correctly
    val writtenTestData = fromSequenceFile[BytesWritable, DoubleWritable](outPath)
    persist(writtenTestData.materialize).toSeq.sorted must_== Seq[(BytesWritable, DoubleWritable)](("a".getBytes, 1.0), ("b".getBytes, 2.0))
  }

  "Expecting exception when Writing FloatWritable -> BooleanWritable, Reading Text -> BooleanWritable" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // can not create a new DList containing Text because of issue # 99
    val testDList: DList[(Float, Boolean)] = DList((1.2f, false), (2.5f, true))
    persist(toSequenceFile(testDList.map((kv: (Float, Boolean)) => (new FloatWritable(kv._1), new BooleanWritable(kv._2))), filePath, true))

    // load test data from the sequence file, then persist to force execution and expect an IOException
    val loadedTestData = fromSequenceFile[Text, BooleanWritable](filePath)
    persist(loadedTestData.materialize) must throwA[IOException]
  }

  "Not checking sequence file types, and catching the exception in the mapper" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // can not create a new DList containing Text because of issue # 99
    val testDList: DList[(Float, Boolean)] = DList((1.2f, false), (2.5f, true))
    persist(toSequenceFile(testDList.map((kv: (Float, Boolean)) => (new FloatWritable(kv._1), new BooleanWritable(kv._2))), filePath, true))

    // load test data from the sequence file, then persist to force execution and expect a ClassCastException in the mapper
    val loadedTestData = fromSequenceFile[Text, BooleanWritable](List(filePath), false)
    val mapResult = loadedTestData.map(d => try { Right(d._1.charAt(0)) } catch { case e => Left(e.getClass) })
    persist(mapResult.materialize).toSeq must_== Seq(Left(classOf[ClassCastException]), Left(classOf[ClassCastException]))
  }

  /**
   * Helper methods and classes
   */
  def createTempSeqFile[K, V](input: DList[(K, V)])(implicit sc: SC, ks: SeqSchema[K], vs: SeqSchema[V]): String = {
    val initialTmpFile = createTempFile()
    persist(convertToSequenceFile(input, initialTmpFile, true))
    initialTmpFile
  }

  def createTempFile(prefix: String = "iotest")(implicit sc: SC): String = TestFiles.path(TestFiles.createTempFile(prefix))
}
