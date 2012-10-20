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

import org.apache.hadoop.io.Writable
import javassist._
import core._

/** A tagged value for Hadoop values. Specifically this will be a V2 type so must
  * implement the Writable interface. */
abstract class TaggedValue(tag: Int) extends Tagged(tag) with Writable {
  def this() = this(0)
}


/** Companion object for dynamically constructing a subclass of TaggedValue. */
object TaggedValue {
  def apply(name: String, tags: Map[Int, (Manifest[_], WireFormat[_])]): RuntimeClass = {
    val builder = new TaggedValueClassBuilder(name, tags)
    builder.toRuntimeClass
  }
}


/** Class for building TaggedValue classes at runtime. */
class TaggedValueClassBuilder
    (name: String,
     tags: Map[Int, (Manifest[_], WireFormat[_])])
  extends ClassBuilder {

  def className = name

  def extendClass: Class[_] = classOf[TaggedValue]

  def build {

    tags.foreach { case (tag, (m, wt)) =>
      /* 'valueN' - fields for each tagged-type. */
      val valueField = new CtField(pool.get(m.erasure.getName), "value" + tag, ctClass)
      valueField.setModifiers(Modifier.PRIVATE)
      ctClass.addField(valueField)

      /* 'writerN' - WireFormat type class field for each tagged-type. */
      addTypeClassModel(wt, "writer" + tag)
    }

    /* Tagged 'get' method */
    val taggedGetCode =
      "switch($1) {" +
       tags.map { case (t, (m, _)) =>
        "case " + t + ": return " + fromObject("value" + t, m) + ";"
       }.mkString +
       "default: return null; }"
    val getMethod = CtNewMethod.make(pool.get("java.lang.Object"),
                                     "get",
                                     Array(CtClass.intType),
                                     Array(),
                                     "{" + taggedGetCode + "}",
                                     ctClass)
    ctClass.addMethod(getMethod)

    /* Tagged 'set' method */
    val taggedSetCode =
      "setTag($1);" +
      "switch($1) {" +
         tags.map { case (t, (m, _)) =>
          "case " + t + ": value" + t + " = (" + classToJavaTypeString(m.erasure)  + ")" + toObject("$2", m) + "; break;"
         }.mkString +
      "default: break; }"
    val setMethod = CtNewMethod.make(CtClass.voidType,
                                     "set",
                                     Array(CtClass.intType, pool.get("java.lang.Object")),
                                     Array(),
                                     "{" + taggedSetCode + "}",
                                     ctClass)
    ctClass.addMethod(setMethod)

    def toWireCode(t: Int) = "writer" + t + ".toWire(value" + t + ", $1);";

    /* Tagged 'write' method */
    val taggedWriteCode =
      if (tags.size == 1)
        "setTag(0);" +
        toWireCode(0)
      else
        "$1.writeInt(tag());" +
        "switch(tag()) {" +
        tags.map {
          case (t, (m, _)) => "case " + t + ": " + toWireCode(t) + " break;"
        }.mkString +
        "default: break; }"

    val writeMethod = CtNewMethod.make(CtClass.voidType,
                                       "write",
                                       Array(pool.get("java.io.DataOutput")),
                                       Array(),
                                       "{" + taggedWriteCode + "}",
                                       ctClass)
    ctClass.addMethod(writeMethod)

    def toReadCode(t: Int, name: String) = "value" + t + " = (" + name + ")writer" + t + ".fromWire($1);"

    /* Tagged 'readFields' method */
    val taggedReadFieldsCode =
      if (tags.size == 1)
        "setTag(0);"+
        toReadCode(0, classToJavaTypeString(tags(0)._1.erasure))
      else
        "setTag($1.readInt());" +
        "switch(tag()) {" +
        tags.map {
         case (t, (m, _)) =>
           "case " + t + ": " + toReadCode(t, classToJavaTypeString(m.erasure)) + " break;"
        }.mkString +
        "default: break; }"

    val readFieldsMethod = CtNewMethod.make(CtClass.voidType,
                                            "readFields",
                                            Array(pool.get("java.io.DataInput")),
                                            Array(),
                                            "{" + taggedReadFieldsCode + "}",
                                            ctClass)
    ctClass.addMethod(readFieldsMethod)
  }
}
