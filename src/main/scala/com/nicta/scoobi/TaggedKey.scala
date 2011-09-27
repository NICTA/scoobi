/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.io.WritableComparable
import javassist._


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
