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
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import testing.TestFiles
import org.apache.hadoop.io._
import application._
import java.io.IOException

class SeqFileReadWriteSpec extends NictaSimpleJobs {

  "Reading Text -> Text Sequence file" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", "b"), ("c", "d"), ("e", "f")))

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, Text](tmpSeqFile)
    loadedTestData.run.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"), ("e", "f"))
  }

  "Writing Text -> Text Sequence file" >> { implicit sc: SC =>
    val filePath = createTempFile()

    val testDList: DList[(String, String)] = DList(("a", "b"), ("c", "d"))
    persist(toSequenceFile(testDList.map((kv: (String, String)) => (new Text(kv._1), new Text(kv._2))), filePath, overwrite =true))

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, Text](filePath)
    loadedTestData.run.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"))
  }

  "Reading Text -> IntWritable, Writing BytesWritable -> DoubleWritable" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", 1), ("b", 2)))
    val outPath = createTempFile("iotest.out")

    // load test data from the sequence file
    val loadedTestData = fromSequenceFile[Text, IntWritable](tmpSeqFile)
    val mappedTestData = loadedTestData.map(x => (new BytesWritable(x._1.getBytes), new DoubleWritable(x._2.get)))
    persist(toSequenceFile(mappedTestData, outPath, overwrite = true))

    // load data to check it was stored correctly
    val writtenTestData = fromSequenceFile[BytesWritable, DoubleWritable](outPath)
    writtenTestData.run.sorted must_== Seq[(BytesWritable, DoubleWritable)](("a".getBytes, 1.0), ("b".getBytes, 2.0))
  }

  "Expecting exception when Writing FloatWritable -> BooleanWritable, Reading Text -> BooleanWritable" >> { implicit sc: SC =>
    val filePath = createTempFile()

    val testDList: DList[(Float, Boolean)] = DList((1.2f, false), (2.5f, true))
    persist(toSequenceFile(testDList.map((kv: (Float, Boolean)) => (new FloatWritable(kv._1), new BooleanWritable(kv._2))), filePath, overwrite = true))

    // load test data from the sequence file, then persist to force execution and expect an IOException
    val loadedTestData = fromSequenceFile[Text, BooleanWritable](filePath)
    loadedTestData.run must throwAn[IOException]
  }

  "Not checking sequence file types, and catching the exception in the mapper" >> { implicit sc: SC =>
    val filePath = createTempFile()

    val testDList: DList[(Float, Boolean)] = DList((1.2f, false), (2.5f, true))
    persist(toSequenceFile(testDList.map((kv: (Float, Boolean)) => (new FloatWritable(kv._1), new BooleanWritable(kv._2))), filePath, overwrite = true))

    // load test data from the sequence file, then persist to force execution and expect a ClassCastException in the mapper
    val loadedTestData = fromSequenceFile[Text, BooleanWritable](List(filePath), checkKeyValueTypes = false)
    val mapResult = loadedTestData.map(d => try { Right(d._1.charAt(0)): Either[String, Int] } catch { case e => Left(e.getClass.getSimpleName) })
    mapResult.run.toSeq must_== Seq(Left("ClassCastException"), Left("ClassCastException"))
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
