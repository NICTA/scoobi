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

import testing.mutable.NictaSimpleJobs
import testing.TestFiles._
import testing.{TestFiles, InputStringTestFile, TempFiles}
import core.InputConverter
import org.apache.hadoop.io.{Text, LongWritable}
import text.TextSource
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.mapreduce.RecordReader
import impl.plan.DListImpl
import core.InputOutputContext
import impl.plan.comp.CompNodeData._
import java.io.File
import impl.io.FileSystems
import org.apache.hadoop.fs.Path
import org.specs2.matcher.FileMatchers

class InputsOutputsSpec extends NictaSimpleJobs with FileMatchers {

  override def isCluster = false

  "1. an input must only be read once" >> { implicit sc: SC =>
    implicit val fs = sc.fileSystem

    if (sc.isInMemory) {
      var checks = 0
      var reads = 0
      lazy val file = createTempFile("test.input")

      TempFiles.writeLines(file, Seq("1", "2", "3"), isRemote)

      val converter = new InputConverter[LongWritable, Text, String] {
        def fromKeyValue(context: InputContext, k: LongWritable, v: Text) = v.toString
      }
      val source = new TextSource[String](Seq(file.getPath), inputConverter = converter) {
        override def inputCheck(implicit sc: ScoobiConfiguration) {
          checks = checks + 1
          super.inputCheck(sc)
        }
        override def read(reader: RecordReader[_,_], mapContext: InputOutputContext, read: Any => Unit) {
          super.read(reader, mapContext, (a: Any) => { reads = reads + 1; read(a) })
        }

      }
      val input1 = DListImpl[String](source)
      val (input2, input3) = (input1.map(identity), input1.map(identity))
      run(input2, input3)

      "there is only one check" ==> { checks === 1 }
      "there are only 3 reads"  ==> { reads === 3 }
    } else success
  }

  "2. it is possible to read from a file or a directory of files" >> { implicit sc: SC =>
    implicit val fs = sc.fileSystem

    val singleFile = TempFiles.writeLines(createTempFile("test"), Seq("a", "b"), isRemote)
    fromTextFile(singleFile).run.normalise === "Vector(a, b)"

    val directory = path(TempFiles.createTempDir("test").getPath)
    FileSystems.fileSystem.mkdirs(new Path(directory))
    TempFiles.writeLines(new File(directory+"/file1.part"), Seq("a1"), isRemote)
    TempFiles.writeLines(new File(directory+"/file2.part"), Seq("b1"), isRemote)

    fromTextFile(directory).run.normalise === "Vector(a1, b1)"
  }

  "3. it is possible to read files from a list of directories where the directory name gets translated to the key" >> { implicit sc: SC =>
    val directory = path(TempFiles.createTempDir("input").getPath)
    implicit val fs = sc.fileSystem
    fs.mkdirs(new Path(directory+"/year=2014/month=01/day=22"))
    fs.mkdirs(new Path(directory+"/year=2014/month=01/day=23"))

    TempFiles.writeLines(new File(directory+"/year=2014/month=01/day=22/file.part"), Seq("a", "b"), isRemote)
    TempFiles.writeLines(new File(directory+"/year=2014/month=01/day=23/file.part"), Seq("c", "d"), isRemote)

    fromPartitionedTextFiles(directory+"/*/*/*/*").mapKeys { path: String =>
      val parts = path.split("/").dropRight(1).reverse.take(3).map(p => Seq("year=", "month=", "day=").foldLeft(p)(_.replace(_, "")))
      val (year, month, day) = (parts(2), parts(1), parts(0))
      s"$year/$month/$day"
    }.run.normalise === "Vector((2014/01/22,a), (2014/01/22,b), (2014/01/23,c), (2014/01/23,d))"
  }

  "4. it is possible to write files where the key is translated to a specific path" >> { implicit sc: SC =>
    val directory = path(TempFiles.createTempDir("output").getPath)
    val list = DList(("2014/01/22", "a"), ("2014/01/22", "b"), ("2014/01/23", "c"), ("2014/01/23", "d"))

    list.toPartitionedTextFile(directory, partition = (s: String) => s, overwrite = true).run

    Seq("22", "23") must contain((date: String) => new File(directory+"/2014/01/"+date).listFiles.toSeq must contain(aFile).atLeastOnce).forall
  }

  "5. Round-trip of writing and reading partitioned text files" >> { implicit sc: SC =>
    val directory = path(TempFiles.createTempDir("output").getPath)
    val list = DList((Date(2014, 1, 22), "a"), (Date(2014, 1, 22), "b"), (Date(2014, 1, 23), "c"), (Date(2014, 1, 23), "d"))

    val written = list.toPartitionedTextFile(directory, partition = (_:Date).toPath, overwrite = true).run
    val read    = fromPartitionedTextFiles(directory+"/*/*/*/*").mapKeys(Date.fromPath)

    read.run.normalise === written.normalise
  }

  implicit val wf: WireFormat[Date] = mkCaseWireFormat(Date.apply _, Date.unapply _)
}

case class Date(year: Int, month: Int, day: Int) {
  def toPath = s"year=$year/month=$month/day=$day"
}

object Date {
  def fromPath(path: String) = {
    val parts = path.split("/").dropRight(1).reverse.take(3).map(p => Seq("year=", "month=", "day=").foldLeft(p)(_.replace(_, "")))
    Date(year = parts(2).toInt, month = parts(1).toInt, day = parts(0).toInt)
  }
}
