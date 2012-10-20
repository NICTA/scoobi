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
package exec

import org.apache.hadoop.mapreduce.{JobID, Job}
import org.apache.hadoop.mapreduce.task.JobContextImpl
import ChannelsInputFormat._
import io._
import impl.util.JarBuilder
import scala.collection.JavaConversions._
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import rtt._
import impl.Configurations._
import application.{LocalHadoop, ScoobiConfiguration}
import testing.mutable.UnitSpecification
import ScoobiConfiguration._

class ChannelsInputFormatSpec extends UnitSpecification with Mockito {
                                                                        """
Several input formats can be grouped as one `ChannelsInputFormat` class.""".endp

  "Each input format can be configured as an input channel on a job's configuration" >> {
    implicit val sc = (new ScoobiConfiguration).setAsLocal
    val job = new Job(sc, "id")
    val jarBuilder = mock[JarBuilder]

    val configuration = configureSources(job, jarBuilder, List(ConstantStringDataSource("one"), aBridgeStore)).toMap.
                          showAs(_.toList.sorted.mkString("\n")).evaluate

    "the input format class must be set as 'ChannelsInputFormat' " >> {
      job.getInputFormatClass.getSimpleName must_== "ChannelsInputFormat"
    }
    "there must be a new runtime class per BridgeStore DataSource, added to the JarBuilder" >> {
      there was one(jarBuilder).addRuntimeClass(any[RuntimeClass])
    }
    "there must be one scoobi input format for each source" >> {
      configuration must havePair("scoobi.input.formats" ->
                                  ("0;com.nicta.scoobi.io.ConstantStringInputFormat,"+
                                   "1;org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"))
    }
    "all the configuration values of a source must be prefixed with scoobi.input 'channel id'" >> {
      configuration must haveKeys("scoobi.input0:mapred.constant.string",
                                  "scoobi.input1:mapred.input.dir")
    }
    "sources can configure distributed cache files. They must be prefixed by channel id in the configuration" >> {
      configuration must haveKeys("scoobi.input0:mapred.cache.files",
                                  "scoobi.input1:mapred.cache.files")
    }

  }

  "Getting the splits for a ChannelsInputFormat" >> {

    "gets the splits for each channel id" >> {
      getSplits(ConstantStringDataSource("one"), ConstantStringDataSource("two")) must have size(2)
    }
    "return no splits when channeling a format returning no splits" >> {
      getSplits(FailingDataSource()) must beEmpty
    }

    def getSplits(sources: DataSource[_,_,_]*) = new ChannelsInputFormat[String, Int].getSplits(jobContextFor(sources:_*)).toSeq

    def jobContextFor(sources: DataSource[_,_,_]*) = {
      implicit val sc = new ScoobiConfiguration
      val job = new Job(sc, "id")
      val jarBuilder = mock[JarBuilder]
      val configuration = configureSources(job, jarBuilder, List(sources:_*))
      new JobContextImpl(configuration, new JobID)
    }
  }

  lazy val aBridgeStore = {
    val bs = BridgeStore[String]()
    bs.rtClass = Some(new RuntimeClass("java.lang.String", classOf[String], Array[Byte]()))
    bs
  }

  def scoobiArgs = Seq[String]()
}
