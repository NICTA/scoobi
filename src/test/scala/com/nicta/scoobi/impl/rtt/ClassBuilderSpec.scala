package com.nicta.scoobi
package impl
package rtt

import testing.mutable.UnitSpecification
import Scoobi._
import org.apache.hadoop.io.Writable
import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import org.specs2.mutable.Tables

class ClassBuilderSpec extends UnitSpecification with Tables {

  "A ScoobiWritable class can be built for a given WireFormat instance" >> {
    val builder = new ScoobiWritableClassBuilder("scoobi", implicitly[Manifest[String]], implicitly[WireFormat[String]])

    normalise(builder.toString) === normalise {
      """|class scoobi extends class com.nicta.scoobi.impl.rtt.ScoobiWritable {
        |  static private core.WireFormatImplicits$StringWireFormat writer =
        |    (core.WireFormatImplicits$StringWireFormat) impl.util.Serialiser$.MODULE$.fromByteArray();
        |
        |  void write ($1: DataOutput) {
        |    writer.toWire((get()), $1);
        |  }
        |
        |  void readFields ($1: DataInput) {
        |    set(writer.fromWire($1));
        |  }
        |
        |  String toString () {
        |    return get().toString();
        |  }
        |}
      """.stripMargin
    }
  }

  "ScoobiWritable classes can be created for any WireFormat instance" >> {

    "Manifest"           | "WireFormat"         | "value"            |>
     mf[String]          ! wf[String]           ! "test"             |
     mf[(String, Int)]   ! wf[(String, Int)]    ! ("test", 2)        | { (mf, wf, value) =>
      serialiseAndDeserialise(value, mf, wf)
    }
  }

  def serialiseAndDeserialise(value: AnyRef, mf: Manifest[_], wf: WireFormat[_]) = {
    val builder = new ScoobiWritableClassBuilder(mf.hashCode.toString, mf, wf)
    val writable = builder.toClass.newInstance.asInstanceOf[ScoobiWritable[AnyRef]]
    writable.set(value)

    val out = new ByteArrayOutputStream
    writable.write(new DataOutputStream(out))

    val in = new ByteArrayInputStream(out.toByteArray)
    writable.readFields(new DataInputStream(in))

    writable.get === value
  }
  // remove unncessary noise for doing a string comparison
  def normalise(string: String) =
    string.trim.replaceAll("new byte\\[\\]\\{.*\\}", "").
                replace("com.nicta.scoobi.", "").
                replace("java.io.", "" ).
                replace("java.lang.", "")


  def mf[T : Manifest]   = implicitly[Manifest[T]]
  def wf[T : WireFormat] = implicitly[WireFormat[T]]
}

