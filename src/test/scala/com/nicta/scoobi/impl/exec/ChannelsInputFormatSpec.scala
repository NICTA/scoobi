package com.nicta.scoobi.impl.exec

import org.specs2.mutable.Specification
import ChannelsInputFormat._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.io.ConstantStringDataSource
import org.apache.hadoop.mapreduce.Job
import com.nicta.scoobi.impl._
import com.nicta.scoobi.impl.util.JarBuilder
import org.specs2.mock.Mockito
import plan.AST.Load
import rtt.RuntimeClass

class ChannelsInputFormatSpec extends Specification with Mockito {

  """
    Several input formats can be grouped as one `ChannelsInputFormat` class.""".txt

  "Each input format can be configured as an input channel on a job's configuration" >> {
    val job = new Job(new Configuration, "id")
    val jarBuilder = mock[JarBuilder]
    configureSources(job, jarBuilder, List(ConstantStringDataSource("one"), aBridgeStore))
    val configuration = job.getConfiguration.toMap
       // uncomment to read the failure messages more easily
       // .filter { case (k, v) => k startsWith "scoobi" }

    "the input format class must be set" >> {
      job.getInputFormatClass.getSimpleName must_== "ChannelsInputFormat"
    }
    "there must be a new runtime class per BridgeStore DataSource" >> {
      there was one(jarBuilder).addRuntimeClass(any[RuntimeClass])
    }
    "there must be one scoobi input format for each source" >> {
      configuration must havePair("scoobi.input.formats" ->
                                  ("0;com.nicta.scoobi.io.ConstantStringDataSource$ConstantStringInputFormat,"+
                                   "1;org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"))
    }
    "all the configuration values of a source must be prefixed with scoobi.input<channel id>" >> {
      configuration must haveKeys("scoobi.input0:mapred.constant.string",
                                  "scoobi.input1:mapred.input.dir")
    }
    "each source must update its distributed cache files" >> {
      configuration must haveKeys("scoobi.input0:mapred.cache.files",
                                  "scoobi.input1:mapred.cache.files")
    }

  }

  lazy val aBridgeStore = {
    val bs = BridgeStore[String](Load())
    bs.rtClass = Some(new RuntimeClass("java.lang.String", classOf[String], Array[Byte]()))
    bs
  }

}
