package com.nicta.scoobi
package impl
package util

import org.specs2.specification._
import script.Specification
import testing.HadoopExamples
import com.nicta.scoobi.core.ScoobiConfiguration
import org.apache.hadoop.conf.Configuration

class DistCacheSpec extends Specification with Groups { def is = """

 The DistCache object can be object to serialise objects and push them to Hadoop's distributed cache

 + pushObject/pullObject must bring back the same object
 + when an object is push, we try to deserialise it right away and throw an exception if this is not possible


                                                     """
 "dist cache" - new group {
   eg := {
     val configuration = new Configuration
     DistCache.pushObject(configuration, "hello world", "tag1")
     DistCache.pullObject[String](configuration, "tag1") === "hello world"
   }
 }

}
