package com.nicta.scoobi
package impl
package util

import org.specs2.specification._
import script.SpecificationLike
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.testing.UnitSpecification

class DistCacheSpec extends UnitSpecification with SpecificationLike with Groups { def is = s2"""

 The DistCache object can be object to serialise objects and push them to Hadoop's distributed cache

 + pushObject/pullObject must bring back the same object
 + when an object is push, we try to deserialise it right away and throw an exception if this is not possible


                                                     """
 "dist cache" - new group {
   eg := {
     val configuration = new Configuration
     DistCache.pushObject(configuration, "hello world", "tag1")
     DistCache.pullObject[String](configuration, "tag1") must beSome("hello world")
   }

   eg := {
     DistCache.pushObject(new Configuration, getClass.getClassLoader, "tag1") must throwAn[Exception]
   }
 }

}
