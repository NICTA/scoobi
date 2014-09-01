package com.nicta.scoobi.io.thrift

import java.io._

import com.nicta.scoobi.Scoobi._
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class ThriftSchemaSpec extends Specification with ScalaCheck {

  "WireFormat bidirectional" >> prop((s: String) => {
    implicit val wf = implicitly[WireFormat[MyThrift]]
    val a = new MyThrift(s)
    val out = new ByteArrayOutputStream()
    wf.toWire(a, new DataOutputStream(out))
    wf.fromWire(new DataInputStream(new ByteArrayInputStream(out.toByteArray))) ==== a
  })

  "SeqSchema bidirectional" >> prop((s: String) => {
    implicit val ss = implicitly[SeqSchema[MyThrift]]
    val a = new MyThrift(s)
    ss.fromWritable(ss.toWritable(a)) ==== a
  })
}