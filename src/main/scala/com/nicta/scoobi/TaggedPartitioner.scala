/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.mapred.Partitioner
import javassist._


/** Custom paritioner for tagged key-values. */
abstract class TaggedPartitioner extends Partitioner[TaggedKey, TaggedValue]


/** Companion object for dynamically constructing a subclass of TaggedPartitioner. */
object TaggedPartitioner {

  def apply(name: String, numTags: Int): RuntimeClass = {
    val builder = new TaggedPartitionerClassBuilder(name, numTags)
    builder.toRuntimeClass
  }
}


/** Class for building TaggedPartitioner classes at runtime. */
class TaggedPartitionerClassBuilder(name: String, numTags: Int) extends ClassBuilder {

  def className = name

  def extendClass: Class[_] = classOf[TaggedPartitioner]

  def build = {

    /* 'getPartition' - do hash paritioning on the key value that is tagged. */
    val getPartitionCode =
      "int tag = ((com.nicta.scoobi.TaggedKey)$1).tag();" +
      "switch(tag) {" +
        (0 to numTags - 1).map { t =>
          "case " + t + ": return (((com.nicta.scoobi.TaggedKey)$1).get(tag).hashCode() & Integer.MAX_VALUE) % $3;"
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

    /* 'configure' - required as Partitioner inherits from JobConfigurable. Make it empty. */
    ctClass.addMethod(CtNewMethod.make(CtClass.voidType,
                                       "configure",
                                       Array(pool.get("org.apache.hadoop.mapred.JobConf")),
                                       Array(),
                                       "{}",
                                       ctClass))
  }
}
