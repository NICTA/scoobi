package com.nicta.scoobi
package impl
package rtt

import testing.mutable.UnitSpecification
import Scoobi._
import org.apache.hadoop.io.Writable
import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import org.specs2.mutable.Tables
import java.util.UUID

class MetadataClassBuilderSpec extends UnitSpecification with Tables {

  """
     When a builder class is created, it has a specific name, given at runtime, and some metadata that is stored through the
     Distributed file cache with a specific path using the class name
  """ >> {
    val builder = new MetadataClassBuilder[MetadataScoobiWritable]("specificName", wf[String])

    normalise(builder.toString) ===
      """|class specificName extends com.nicta.scoobi.impl.rtt.AbstractScoobiWritable {
         |  java.lang.String metadataPath () {
         |    return "...scoobi.class.specificName";
         |  }
         |}
      """.stripMargin.trim
  }

  "ScoobiWritable classes can be created for any WireFormat instance" >> {
    "WireFormat"          | "value"            |>
     wf[String]           ! "test"             |
     wf[(String, Int)]    ! ("test", 2)        | checkScoobiWritable
  }

  "TaggedValue classes can be created for any WireFormat instance" >> {
    "WireFormat"          | "value"            |>
     wf[String]           ! "test"             |
     wf[(String, Int)]    ! ("test", 2)        | checkTaggedValue
  }

  "TaggedKey classes can be created for any WireFormat instance" >> {
    "WireFormat"          | "Grouping"           | "value"            |>
     wf[String]           ! gp[String]           ! "test"             |
     wf[(String, Int)]    ! gp[(String, Int)]    ! ("test", 2)        | checkTaggedKey
  }

  "TaggedGroupingComparator classes can be created for any WireFormat instance" >> {
    "WireFormat"          | "Grouping"           | "value"            |>
     wf[String]           ! gp[String]           ! "test"             |
     wf[(String, Int)]    ! gp[(String, Int)]    ! ("test", 2)        | checkTaggedGroupingComparator
  }

  "TaggedPartitioner classes can be created for any WireFormat instance" >> {
    "WireFormat"          | "Grouping"           | "value"            |>
     wf[String]           ! gp[String]           ! "test"             |
     wf[(String, Int)]    ! gp[(String, Int)]    ! ("test", 2)        | checkTaggedPartition
  }

  def checkScoobiWritable = (wf: WireFormat[_], value: Any) => {
    val builder = new MetadataClassBuilder[MetadataScoobiWritable](UUID.randomUUID.toString, wf)
    val writable = builder.toClass.newInstance.asInstanceOf[ScoobiWritable[Any]]
    writable.set(value)
    serialiseAndDeserialise(writable)
    writable.get === value
  }

  def checkTaggedValue = (wf: WireFormat[_], value: Any) => {
    val builder = new MetadataClassBuilder[MetadataTaggedValue](UUID.randomUUID.toString, Map(0 -> Tuple1(wf)))
    val writable = builder.toClass.newInstance.asInstanceOf[TaggedValue]
    writable.set(0, value)
    serialiseAndDeserialise(writable)
    writable.get(0) === value
  }

  def checkTaggedKey = (wf: WireFormat[_], gp: Grouping[_], value: Any) => {
    val builder = new MetadataClassBuilder[MetadataTaggedKey](UUID.randomUUID.toString, Map(0 -> (wf, gp)))
    val key = builder.toClass.newInstance.asInstanceOf[TaggedKey]
    key.set(0, value)
    serialiseAndDeserialise(key)

    key.get(0) === value
    key.compareTo(key) === 0
  }

  def checkTaggedGroupingComparator = (wf: WireFormat[_], gp: Grouping[_], v: Any) => {
    val builder = new MetadataClassBuilder[MetadataTaggedGroupingComparator](UUID.randomUUID.toString, Map(3 -> (wf, gp)))
    val grouping = builder.toClass.newInstance.asInstanceOf[TaggedGroupingComparator]

    val keyBuilder = new MetadataClassBuilder[MetadataTaggedKey](UUID.randomUUID.toString, Map(3 -> (wf, gp)))
    val key = keyBuilder.toClass.newInstance.asInstanceOf[TaggedKey]
    key.set(3, v)

    grouping.compare(key, key) === 0
  }

  def checkTaggedPartition = (wf: WireFormat[_], gp: Grouping[_], v: Any) => {
    val builder = new MetadataClassBuilder[MetadataTaggedPartitioner](UUID.randomUUID.toString, Map(3 -> (wf, gp)))
    val partitioner = builder.toClass.newInstance.asInstanceOf[TaggedPartitioner]

    val keyBuilder = new MetadataClassBuilder[MetadataTaggedKey](UUID.randomUUID.toString, Map(3 -> (wf, gp)))
    val key = keyBuilder.toClass.newInstance.asInstanceOf[TaggedKey]
    key.set(3, v)

    val valueBuilder = new MetadataClassBuilder[MetadataTaggedValue](UUID.randomUUID.toString, Map(3 -> Tuple1(wf)))
    val value        = valueBuilder.toClass.newInstance.asInstanceOf[TaggedValue]
    value.set(3, v)

    partitioner.getPartition(key, value, 2) must be_>=(0)
  }

  def serialiseAndDeserialise(writable: Writable) {
    val out = new ByteArrayOutputStream
    writable.write(new DataOutputStream(out))

    val in = new ByteArrayInputStream(out.toByteArray)
    writable.readFields(new DataInputStream(in))
  }

  // remove unnecessary noise for doing a string comparison
  def normalise(string: String) = string.trim.replaceAll("return \".+scoobi", "return \"...scoobi")

  def mf[T : Manifest]   = implicitly[Manifest[T]]
  def wf[T : WireFormat] = implicitly[WireFormat[T]]
  def gp[T : Grouping]   = implicitly[Grouping[T]]
}

