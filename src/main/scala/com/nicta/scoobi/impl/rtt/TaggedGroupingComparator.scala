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

import org.apache.hadoop.io.RawComparator
import javassist._
import core._

/** Custom GroupingComparator for tagged keys. */
abstract class TaggedGroupingComparator extends RawComparator[TaggedKey]


/** Companion object for dynamically constructing a subclass of TaggedGroupingComparator. */
object TaggedGroupingComparator {

  def apply(name: String, tags: Map[Int, (Manifest[_], WireFormat[_], Grouping[_])]) : RuntimeClass = {
    val builder = new TaggedGroupingComparatorClassBuilder(name, tags)
    builder.toRuntimeClass
  }
}


/** Class for building TaggedGroupingComparator classes at runtime. */
class TaggedGroupingComparatorClassBuilder
    (name: String,
     tags: Map[Int, (Manifest[_], WireFormat[_], Grouping[_])])
  extends ClassBuilder {

  def className = name

  def extendClass: Class[_] = classOf[TaggedGroupingComparator]

  def build {

    tags.foreach { case (t, (_, wt, grp)) =>
      /* 'writerN' - WireFormat type class field for each tagged-type. */
      addTypeClassModel(wt, "writer" + t)
      /* 'grouperN' - Grouping type class field for each tagged-type. */
      addTypeClassModel(grp, "grouper" + t)
    }

    def addBufferField(name: String) = {
      val dibClassName = "org.apache.hadoop.io.DataInputBuffer"
      ctClass.addField(new CtField(pool.get(dibClassName), name, ctClass),
                       CtField.Initializer.byExpr("new " + dibClassName + "();"))
    }
    addBufferField("buffer1")
    addBufferField("buffer2")

    /* 'compare' - if the tags are the same for the two TaggedKeys, use the Grouping's
     * groupCompare method to compare the two keys. */

    def genFromObj(t: Int, m: Manifest[_])=
      "return grouper" + t + ".groupCompare(" +fromObject("(" + classToJavaTypeString(m.erasure) + ")writer" + t + ".fromWire(buffer1)", m) + ", " +
        fromObject("(" + classToJavaTypeString(m.erasure) + ")writer" + t + ".fromWire(buffer2)", m) + ");"

    val compareCode =
      "buffer1.reset($1, $2, $3);" +
      "buffer2.reset($4, $5, $6);" +
      (if (tags.size == 1) {
        genFromObj(0, tags(0)._1)
      } else {
        "int tag1 = buffer1.readInt();" +
        "int tag2 = buffer2.readInt();" +
        "if (tag1 == tag2) {" +
          "switch(tag1) {" +
            tags.map { case (t, (m, _, _)) =>
              "case " + t + ": " +genFromObj(t, m)
            } .mkString +
            "default: return 0;" +
          "}" +
        "} else {" +
          "return tag1 - tag2;" +
        "}"
      })


    val compareMethod = CtNewMethod.make(CtClass.intType,
                                         "compare",
                                         Array(pool.get("byte[]"),
                                               CtClass.intType,
                                               CtClass.intType,
                                               pool.get("byte[]"),
                                               CtClass.intType,
                                               CtClass.intType),
                                         Array(),
                                         "{" + compareCode + "}",
                                         ctClass)
    ctClass.addMethod(compareMethod)
  }
}
