/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.rtt

import org.apache.hadoop.io.WritableComparable
import javassist._

import com.nicta.scoobi.HadoopWritable


/** A tagged value for Hadoop keys. Specifically this will be a K2 type so must
  * implement the WritableComparable interface. */
abstract class TaggedKey(tag: Int) extends Tagged(tag) with WritableComparable[TaggedKey] {
  def this() = this(0)
}


/** Companion object for dynamically constructing a subclass of TaggedKey. */
object TaggedKey {

  def apply(name: String, tags: Map[Int, (Manifest[_], HadoopWritable[_], Ordering[_])]): RuntimeClass = {
    val builder = new TaggedKeyClassBuilder(name, tags)
    builder.toRuntimeClass
  }
}


/** Class for building TaggedKey classes at runtime. */
class TaggedKeyClassBuilder
    (name: String,
     tags: Map[Int, (Manifest[_], HadoopWritable[_], Ordering[_])])
  extends TaggedValueClassBuilder(name, tags.map{case (t, (m, wt, _)) => (t, (m, wt))}.toMap) {

  override def extendClass: Class[_] = classOf[TaggedKey]

  override def build = {

    /* TaggedKey sub-classes are super-classes of TaggedValue sub-classes. */
    super.build

    tags.foreach { case (t, (_, _, ord)) =>
      /* 'comparerN' - Ordering type class field for each tagged-type. */
      addTypeClassModel(ord, "comparer" + t)
    }

    /* 'compareTo' - peform comparison on tags first then, if equal, perform
     * comparison on selected tagged value using 'comparerN'. */
    val compareToCode =
      className + " tk = (" + className + ")$1;" +
      "if (tk.tag() == this.tag()) {" +
        "switch(this.tag()) {" +
          tags.keys.map(t => "case " + t + ": return comparer" + t + ".compare(value" + t + ", tk.value" + t + ");").mkString +
          "default: return 0;" +
        "}" +
      "} else {" +
        "return this.tag() - tk.tag();" +
      "}"
    val compareToMethod = CtNewMethod.make(CtClass.intType,
                                           "compareTo",
                                           Array(pool.get("java.lang.Object")),
                                           Array(),
                                           "{" + compareToCode + "}",
                                           ctClass)
    ctClass.addMethod(compareToMethod)
  }
}
