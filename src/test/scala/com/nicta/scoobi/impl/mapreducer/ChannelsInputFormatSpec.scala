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
package impl
package mapreducer

import org.apache.hadoop.mapreduce._
import ChannelsInputFormat._
import scala.collection.JavaConversions._
import org.specs2.mock.Mockito

import core._
import com.nicta.scoobi.io._
import testing.mutable.UnitSpecification
import com.nicta.scoobi.impl.rtt._
import Configurations._
import impl.ScoobiConfiguration
import WireFormat._
import rtt.RuntimeClass
import org.apache.hadoop.filecache.DistributedCache
import com.nicta.scoobi.testing.{TempFiles, TestFiles}
import impl.util.Compatibility
import com.nicta.scoobi.io.text.TextSource
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.io.text.TextSource
import com.nicta.scoobi.impl.rtt.RuntimeClass
import java.io.File

class ChannelsInputFormatSpec extends UnitSpecification with Mockito {
                                                                        """
Several input formats can be grouped as one `ChannelsInputFormat` class.""".endp

  "Each input format can be configured as an input channel on a job's configuration" >> {
    implicit val sc = ScoobiConfiguration().setAsLocal
    val job = new Job(sc.configuration, "id")
    val jarBuilder = mock[JarBuilder]

    val (source1, source2) = (stringDataSource("one"), aBridgeStore)
    val configured = configureSources(job, jarBuilder, Seq(source1, source2)).toMap
    val configuration = configured.showAs(_.toList.sorted.filter(_._1.startsWith("scoobi")).mkString("\n")).evaluate

    val mapredConfiguration = configured.showAs(_.toList.sorted.filter(_._1.startsWith("mapred")).mkString("\n")).evaluate


      "the input format class must be set as 'ChannelsInputFormat' " >> {
      job.getInputFormatClass.getSimpleName must_== "ChannelsInputFormat"
    }
    "there must be a new runtime class per BridgeStore DataSource, added to the JarBuilder" >> {
      there was one(jarBuilder).addRuntimeClass(any[RuntimeClass])
    }
    "there must be one scoobi input format for each source" >> {
      configuration must havePair("scoobi.input.formats" ->
                                  (source1.id+";com.nicta.scoobi.io.ConstantStringInputFormat,"+
                                   source2.id+";org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"))
    }
    "all the configuration values of a source must be prefixed with scoobi.input 'channel id'" >> {
      configuration must haveKeys("scoobi.input"+source1.id+":mapred.constant.string",
                                  "scoobi.input"+source2.id+":mapred.input.dir")
    }
    "sources can configure distributed cache files but they are not prefixed by channel id in the configuration" >> {
      mapredConfiguration must haveKey("mapred.cache.files")
      "there must be 2 cached files" ==> {
        mapredConfiguration.map((_:Map[String,String])("mapred.cache.files").split(",")) must haveSize(2)
      }
    }

  }

  "Getting the splits for a ChannelsInputFormat" >> {
    "gets the splits for each source" >> {
      getSplits(ConstantStringDataSource("one"), ConstantStringDataSource("two")) must have size(2)
    }
    "return no splits when the data source returns no splits" >> {
      getSplits(FailingDataSource()) must beEmpty
    }
    "only return one split if only one data source contain paths (see #283)" >> {
      val inputDir = TestFiles.createTempDir("path")(ScoobiConfiguration())
      new File(inputDir+"/test").createNewFile()
      getSplits(TextSource(Seq(inputDir.getPath)), TextSource(Seq())) must haveSize(1)
    }
  }

  lazy val aBridgeStore = BridgeStore[String]("id", wireFormat[String])


  def getSplits(sources: DataSource[_,_,_]*) = new ChannelsInputFormat[String, Int].getSplits(jobContextFor(sources:_*)).toSeq

  def jobContextFor(sources: DataSource[_,_,_]*) = {
    implicit val sc = ScoobiConfiguration()
    val job = new Job(sc.configuration, "id")
    val jarBuilder = mock[JarBuilder]
    val configuration = configureSources(job, jarBuilder, Seq(sources:_*))
    Compatibility.newJobContext(configuration, new JobID)
  }

  def stringDataSource(string: String) =  new ConstantStringDataSource(string) {
    override def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
      super.inputConfigure(job)
      val file = TestFiles.createTempFile("cache")
      DistributedCache.addCacheFile(file.toURI, job.getConfiguration)
    }
  }

  def scoobiArgs = Seq[String]()
}
