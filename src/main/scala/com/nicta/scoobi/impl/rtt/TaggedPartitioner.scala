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
package rtt

import org.apache.hadoop.mapreduce.Partitioner
import javassist._
import core._

/** Custom partitioner for tagged key-values. */
abstract class TaggedPartitioner extends Partitioner[TaggedKey, TaggedValue]


/** Companion object for dynamically constructing a subclass of TaggedPartitioner. */
object TaggedPartitioner {

  def apply(name: String, tags: Map[Int, (Manifest[_], WireFormat[_], Grouping[_])]): RuntimeClass = {
    val builder = new TaggedPartitionerClassBuilder(name, tags)
    builder.toRuntimeClass
  }
}


/** Class for building TaggedPartitioner classes at runtime. */
class TaggedPartitionerClassBuilder
    (name: String,
     tags: Map[Int, (Manifest[_], WireFormat[_], Grouping[_])])
  extends ClassBuilder {

  def className = name

  def extendClass: Class[_] = classOf[TaggedPartitioner]

  def build {

    tags.foreach { case (t, (_, _, grp)) =>
      /* 'grouperN' - Grouping type class field for each tagged-type. */
      addTypeClassModel(grp, "grouper" + t)
    }

    /* 'getPartition' - do hash partitioning on the key value that is tagged. */
    val getPartitionCode =
      "int tag = ((com.nicta.scoobi.impl.rtt.TaggedKey)$1).tag();" +
      "switch(tag) {" +
        (0 to tags.size - 1).map { t =>
          "case " + t + ": return grouper" + t + ".partition(((com.nicta.scoobi.impl.rtt.TaggedKey)$1).get(tag), $3);"
        }.mkString +
        "default: return 0;" +
      "}"
    val getPartitionMethod = CtNewMethod.make(CtClass.intType,
                                              "getPartition",
                                              Array(pool.get("java.lang.Object"),
                                                    pool.get("java.lang.Object"),
                                                    CtClass.intType),
                                              Array(),
                                              "{" + getPartitionCode + "}",
                                              ctClass)
    ctClass.addMethod(getPartitionMethod)
  }
}
