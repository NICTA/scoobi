/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.io._
import javassist._


/** The super-class of all "key" types used in Hadoop jobs. */
abstract class ScoobiWritableComparable[A](private var x: A)
    extends WritableComparable[ScoobiWritableComparable[A]] { self =>
  def this() = this(null.asInstanceOf[A])
  def get: A = x
  def set(x: A) = { self.x = x }
}


/** Companion object for dynamically constructing a subclass of ScoobiWritableComparable. */
object ScoobiWritableComparable {

  /** Constructs a subclass of ScoobiWritableComparable dynamically.
    *
    * A ScoobiWritableComparable subclass is constructed based on a HadoopWritableComparable
    * typeclass model imiplicit parameter. Using this model object, the Hadoop WritableComparable
    * methods 'write', 'readFields' and 'compareTo' can be generated. */
  def apply[A](name: String, witness: A)
              (implicit m: Manifest[A], wt: HadoopWritable[A], ord: Ordering[A]): RuntimeClass = {

    val cb = new ClassBuilder(name, classOf[ScoobiWritableComparable[_]]) {
      override def toRuntimeClass(): RuntimeClass = {
        /*
         * Deal with HadoopWritable type class. TODO - this is the same code as for ScoobiWritable!
         */
        addTypeClassModel(wt, "writer")

        /* 'write' - method to override from Writable */
        val writeMethod = CtNewMethod.make(CtClass.voidType,
                                           "write",
                                           Array(pool.get("java.io.DataOutput")),
                                           Array(),
                                           "writer.toWire(" + getterCode(m) + ", $1);",
                                           ctClass)
        ctClass.addMethod(writeMethod)

        /* 'readFields' = method to override from Writable */
        val readFieldsMethod = CtNewMethod.make(CtClass.voidType,
                                                "readFields",
                                                Array(pool.get("java.io.DataInput")),
                                                Array(),
                                                setterCode("writer.fromWire($1)", m) + ";",
                                                ctClass)
        ctClass.addMethod(readFieldsMethod)

        /* 'toString' = method to override from Writable */
        val toStringMethod = CtNewMethod.make(pool.get("java.lang.String"),
                                              "toString",
                                              Array(),
                                              Array(),
                                              "return writer.show(" + getterCode(m) + ");",
                                              ctClass)
        ctClass.addMethod(toStringMethod)


        /*
         * Deal with Ordering type class.
         */
        addTypeClassModel(ord, "comparer")

        /* 'cmp' - compare method */
        val cmpMethod = CtNewMethod.make(CtClass.intType,
                                         "compareTo",
                                         Array(pool.get("java.lang.Object")),
                                         Array(),
                                         "return comparer.compare(this.get(), ((" + newClassName + ")$1).get());",
                                         ctClass)
        ctClass.addMethod(cmpMethod)

        super.toRuntimeClass()
      }
    }

    cb.toRuntimeClass()
  }
}
