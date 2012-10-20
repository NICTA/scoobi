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

import org.apache.hadoop.io._
import javassist._
import scala.collection.mutable.{Map => MMap}
import core._

/** The super-class of all "value" types used in Hadoop jobs. */
abstract class ScoobiWritable[A](private var x: A) extends Writable { self =>
  def this() = this(null.asInstanceOf[A])
  def get: A = x
  def set(x: A) { self.x = x }
}


/** Constructs a subclass of ScoobiWritable dynamically. */
object ScoobiWritable {

  val builtClasses: MMap[String, RuntimeClass] = MMap.empty

  def apply(name: String, m: Manifest[_], wt: WireFormat[_]): RuntimeClass = {
    if (!builtClasses.contains(name)) {
      val builder = new ScoobiWritableClassBuilder(name, m, wt)
      builtClasses += (name -> builder.toRuntimeClass)
    }

    builtClasses(name)
  }

  def apply[A](name: String, witness: A)(implicit m: Manifest[A], wt: WireFormat[A]): RuntimeClass = {
    apply(name, m, wt)
  }
}


/** A ScoobiWritable subclass is constructed based on a WireFormat typeclass
  * model implicit parameter. Using this model object, the Hadoop Writable methods
  * 'write' and 'readFields' can be generated. */
class ScoobiWritableClassBuilder(name: String, m: Manifest[_], wt: WireFormat[_]) extends ClassBuilder {

  def className = name

  def extendClass: Class[_] = classOf[ScoobiWritable[_]]

  def build = {
    /* Deal with WireFormat type class. */
    addTypeClassModel(wt, "writer")

    /* 'write' - method to override from Writable */
    val writeMethod = CtNewMethod.make(CtClass.voidType,
                                       "write",
                                       Array(pool.get("java.io.DataOutput")),
                                       Array(),
                                       "writer.toWire(" + toObject("get()", m) + ", $1);",
                                       ctClass)
    ctClass.addMethod(writeMethod)

    /* 'readFields' = method to override from Writable */
    val readFieldsMethod = CtNewMethod.make(CtClass.voidType,
                                            "readFields",
                                            Array(pool.get("java.io.DataInput")),
                                            Array(),
                                            "set(" + fromObject("writer.fromWire($1)", m) + ");",
                                            ctClass)
    ctClass.addMethod(readFieldsMethod)

    /* 'toString' = method to override from Writable */
    val toStringMethod = CtNewMethod.make(pool.get("java.lang.String"),
                                          "toString",
                                          Array(),
                                          Array(),
                                          "return get().toString();",
                                          ctClass)
    ctClass.addMethod(toStringMethod)
  }
}
