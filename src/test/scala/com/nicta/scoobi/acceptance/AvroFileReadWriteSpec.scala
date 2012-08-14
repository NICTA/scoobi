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
import testing.{NictaSimpleJobs, TestFiles}
import impl.exec.JobExecException

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io._

import org.specs2.matcher.{HaveTheSameElementsAs, MatchResult}

import org.apache.avro.{AvroTypeException, Schema}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord, GenericData}
import org.apache.avro.file.DataFileWriter

class AvroFileReadWriteSpec extends NictaSimpleJobs {

  skipAll

  "Reading (Int, Seq[(Float, String)], Map[String, Int]) Avro file" >> { implicit sc: SC =>
    // create test data
    val testData: Seq[(Int, Seq[(Float, String)], Map[String, Int])] = Seq(
      (1, Seq((3.4f, "abc")), Map("a" -> 5, "b" -> 6)),
      (2, Seq((5.1f, "def")), Map("c" -> 7, "d" -> 8)))

    // store test data in an avro file
    val tmpAvroFile = createTempAvroFile(testData.toDList)

    // load test data from the avro file
    val loadedTestData = fromAvroFile[(Int, Seq[(Float, String)], Map[String, Int])](tmpAvroFile)
    loadedTestData.run must haveTheSameElementsAs(testData, equality)
  }

  "Writing (String, List[(Double,Boolean,String)], Array[Long]) Avro file" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // create test data
    val testData: Seq[(String, List[(Double, Boolean, String)], Array[Long])] = Seq(
      ("abcd", List((6.9d, false, "qwerty")), Array(100l, 200l)),
      ("efghi", List((9.15d, true, "dvorak")), Array(9999l, 11111l)))

    // write the test data out
    persist(toAvroFile(testData.toDList, filePath, overwrite = true))

    // load the test data back, and check
    val loadedTestData: DList[(String, List[(Double, Boolean, String)], Array[Long])] = fromAvroFile(filePath)
    loadedTestData.run must haveTheSameElementsAs(testData, equality)
  }

  "Expecting exception because of miss match in expected and actual schema" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // create test data
    val testData: Seq[(String, List[(Double, Boolean, String)], Array[Long])] = Seq(
      ("abcd", List((6.9d, false, "qwerty")), Array(100l, 200l)),
      ("efghi", List((9.15d, true, "dvorak")), Array(9999l, 11111l)))

    // write the test data out
    persist(toAvroFile(testData.toDList, filePath, overwrite = true))

    // load the test data back, and check
    val loadedTestData: DList[(List[String], Array[Long])] = fromAvroFile(filePath)
    loadedTestData.run must throwAn[AvroTypeException]
  }

  "Not checking schema, and hence expecting an exception in the mapper" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // create test data
    val testData: Seq[(String, List[(Double, Boolean, String)], Array[Long])] = Seq(
      ("abcd", List((6.9d, false, "qwerty")), Array(100l, 200l)),
      ("efghi", List((9.15d, true, "dvorak")), Array(9999l, 11111l)))

    // write the test data out
    persist(toAvroFile(testData.toDList, filePath, overwrite = true))

    // load the test data back, and check
    val loadedTestData: DList[(List[String], Array[Long])] = fromAvroFile(List(filePath), false)
    loadedTestData.run must throwA[JobExecException]
  }

  "Reading a subset of fields that have been written" >> { implicit sc: SC =>
    val filePath = createTempFile()

    // create test data
    val testData: Seq[(String, List[(Double, Boolean, String)], Array[Long])] = Seq(
      ("abcd", List((6.9d, false, "qwerty")), Array(100l, 200l)),
      ("efghi", List((9.15d, true, "dvorak")), Array(9999l, 11111l)))

    val expectedData = testData.map{ t1 =>
      (t1._1, t1._2.map(t2 => (t2._1, t2._2)))
    }

    // write the test data out
    persist(toAvroFile(testData.toDList, filePath, overwrite = true))

    // load the test data back, and check
    val loadedTestData: DList[(String, List[(Double, Boolean)])] = fromAvroFile(filePath)
    loadedTestData.run must haveTheSameElementsAs(expectedData, equality)
  }

  "Avro file written through non scoobi API with a union type in the schema, then read through scoobi" >> { implicit sc: SC =>
    val filePath = new Path(createTempFile())

    val jsonSchema = """{
                         "name": "record1",
                         "type": "record",
                         "fields": [
                           {"name": "v0", "type": [
                             "int",
                             {"name": "record2", "type": "record", "fields": [
                               {"name": "v0", "type": ["null", "string"]}
                             ]}
                           ]},
                           {"name": "v1", "type": "string"},
                           {"name": "v2", "type": "boolean"},
                           {"name": "v3", "type": "double"}
                        ]}"""

    val writerSchema = new Schema.Parser().parse(jsonSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](writerSchema))
    dataFileWriter.create(writerSchema, FileSystem.get(filePath.toUri, sc).create(filePath, true))

    val record = new GenericData.Record(writerSchema)
    record.put("v0", 50)
    record.put("v1", "some test str")
    record.put("v2", true)
    record.put("v3", 3.7)

    dataFileWriter.append(record)
    dataFileWriter.close()

    val loadedTestData: DList[(Long,String,Boolean,Double)] = fromAvroFile(filePath.toString)
    run(loadedTestData) must_== Seq((50, "some test str", true, 3.7))
  }

  /**
   * Helper methods and classes
   */

  def createTempAvroFile[T](input: DList[T])(implicit sc: SC, as: AvroSchema[T]): String = {
    val initialTmpFile = createTempFile()
    persist(toAvroFile(input, initialTmpFile, overwrite = true))
    initialTmpFile
  }

  def createTempFile(prefix: String = "iotest")(implicit sc: SC): String = TestFiles.path(TestFiles.createTempFile(prefix))

  val equality = (t1: Any, t2: Any) => (t1, t2) match {
    case (tt1: Array[_], tt2: Array[_]) => tt1.toSeq == tt2.toSeq
    case (tt1: Iterable[_], tt2: Iterable[_]) => iterablesEqual(tt1, tt2)
    case (tt1: Product, tt2: Product) => productsEqual(tt1, tt2)
    case other => t1 == t2
  }

  def productsEqual(t1: Product, t2: Product): Boolean = {
    if(t1.productArity != t2.productArity) false
    val i1 = t1.productIterator
    val i2 = t2.productIterator
    while (i1.hasNext && i2.hasNext)
      if (!equality(i1.next, i2.next)) false
    true
  }

  def iterablesEqual[T](t1: Iterable[T], t2: Iterable[T]): Boolean = {
    if(t1.size != t2.size) false
    val i1 = t1.iterator
    val i2 = t2.iterator
    while (i1.hasNext && i2.hasNext)
      if (!equality(i1.next, i2.next)) false
    true
  }
}
