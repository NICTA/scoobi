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

import org.apache.hadoop.io._
import java.io.IOException

import Scoobi._
import testing.mutable.NictaSimpleJobs
import testing._
import TempFiles._
import application.Orderings._
import impl.control.Exceptions._

class SeqFileReadWriteSpec extends NictaSimpleJobs {

  "Reading from a text sequence file" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", "b"), ("c", "d"), ("e", "f")))

    // load test data from the sequence file
    fromSequenceFile[Text, Text](tmpSeqFile).run.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"), ("e", "f"))
  }

  "Writing to a text sequence file" >> { implicit sc: SC =>
    val filePath = createTempFilePath("sequence-file")

    DList(("a", "b"), ("c", "d")).map(kv => (new Text(kv._1), new Text(kv._2))).toSequenceFile(filePath).run

    // load test data from the sequence file
    fromSequenceFile[Text, Text](filePath).run.sorted must_== Seq[(Text, Text)](("a", "b"), ("c", "d"))
  }

  "Reading Text -> IntWritable, Writing BytesWritable -> DoubleWritable" >> { implicit sc: SC =>
    // store test data in a sequence file
    val tmpSeqFile = createTempSeqFile(DList(("a", 1), ("b", 2)))
    val outPath    = createTempDir("iotest.out").getPath

    // load test data from the sequence file
    fromSequenceFile[Text, IntWritable](tmpSeqFile).
      map(x => (new BytesWritable(x._1.getBytes), new DoubleWritable(x._2.get))).
      toSequenceFile(outPath, overwrite = true).run

    // load data to check it was stored correctly
    fromSequenceFile[BytesWritable, DoubleWritable](outPath).run.sorted ===
      Seq[(BytesWritable, DoubleWritable)](("a".getBytes, 1.0), ("b".getBytes, 2.0))
  }

  "Expecting exception when Writing FloatWritable -> BooleanWritable, Reading Text -> BooleanWritable" >> { implicit sc: SC =>
    val filePath = TempFiles.createTempFilePath("test")

    DList((1.2f, false), (2.5f, true)).
      map(kv => (new FloatWritable(kv._1), new BooleanWritable(kv._2))).toSequenceFile(filePath, overwrite = true).run

    // load test data from the sequence file, then persist to force execution and expect an IOException
    fromSequenceFile[Text, BooleanWritable](filePath).run must throwAn[IOException]
  }

  "Not checking sequence file types, and catching the exception in the mapper" >> { implicit sc: SC =>
    if (sc.isInMemory) ok
    else {
      val filePath = TempFiles.createTempFilePath("test")

      DList((1.2f, false), (2.5f, true)).
        map(kv => (new FloatWritable(kv._1), new BooleanWritable(kv._2))).toSequenceFile(filePath, overwrite = true).run

      // load test data from the sequence file, then persist to force execution and expect a ClassCastException in the mapper
      fromSequenceFile[Text, BooleanWritable](Seq(filePath), checkKeyValueTypes = false).
        map(d => trye(d._1.charAt(0))(_.getClass.getSimpleName)).run.toSeq ===
        Seq(Left("ClassCastException"), Left("ClassCastException"))
    }
  }

  /**
   * Helper methods and classes
   */
  def createTempSeqFile[K, V](input: DList[(K, V)])(implicit sc: SC, ks: SeqSchema[K], vs: SeqSchema[V]): String = {
    val dir = createTempDir("test").getPath
    persist(input.convertToSequenceFile(dir, overwrite = true))
    dir
  }

  def createTempFile(prefix: String = "iotest")(implicit sc: SC): String = TestFiles.path(TestFiles.createTempFile(prefix))
}
