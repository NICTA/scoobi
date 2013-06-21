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
