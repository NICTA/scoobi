package com.nicta.scoobi.impl.exec

import org.specs2.mutable.Specification
import ChannelsInputFormat._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.io.ConstantStringDataSource
import com.nicta.scoobi.impl._
import com.nicta.scoobi.impl.util.JarBuilder
import org.specs2.mock.Mockito
import plan.AST.Load
import rtt.RuntimeClass
import com.nicta.scoobi.impl.Configurations._
import org.apache.hadoop.mapreduce.{JobID, JobContext, Job}


class ChannelsInputFormatSpec extends Specification with Mockito {

"""
Several input formats can be grouped as one `ChannelsInputFormat` class.""".endp

  "Each input format can be configured as an input channel on a job's configuration" >> {
    val job = new Job(new Configuration, "id")
    val jarBuilder = mock[JarBuilder]
    val configuration = configureSources(job, jarBuilder, List(ConstantStringDataSource("one"), aBridgeStore))
                          .toMap.showAs(_.toList.sorted.mkString("\n")).evaluate

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

    val job = new Job(new Configuration, "id")
    val jarBuilder = mock[JarBuilder]
    val configuration = configureSources(job, jarBuilder, List(ConstantStringDataSource("one"),
                                                               ConstantStringDataSource("two")))
    val jobContext = new JobContext(configuration, new JobID)

    "gets the splits for each channel id" >> {
      new ChannelsInputFormat[String, Int].getSplits(jobContext) must have size(2)
    }
  }

  lazy val aBridgeStore = {
    val bs = BridgeStore[String]()
    bs.rtClass = Some(new RuntimeClass("java.lang.String", classOf[String], Array[Byte]()))
    bs
  }

}
