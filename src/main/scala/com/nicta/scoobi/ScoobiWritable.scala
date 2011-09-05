/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.io._
import javassist._


/** The super-class of all "value" types used in Hadoop jobs. */
abstract class ScoobiWritable[A](private var x: A) extends Writable { self =>
  def this() = this(null.asInstanceOf[A])
  def get: A = x
  def set(x: A) = { self.x = x }
}


/** Companion object for dynamically constructing a subclass of ScoobiWritable. */
object ScoobiWritable {

  /** Constructs a subclass of ScoobiWritable dynamically.
    *
    * A ScoobiWritable subclass is constructed based on a HadoopWritable typeclass
    * model imiplicit parameter. Using this model object, the Hadoop Writable methods
    * 'write' and 'readFields' can be generated. */
  def apply[A](name: String, witness: A)
              (implicit m: Manifest[A], wt: HadoopWritable[A]): RuntimeClass = {

    val cb = new ClassBuilder(name, classOf[ScoobiWritable[_]]) {
      override def toRuntimeClass(): RuntimeClass = {
        /*
         * Deal with HadoopWritable type class.
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

        super.toRuntimeClass()
      }
    }

    cb.toRuntimeClass()
  }
}

