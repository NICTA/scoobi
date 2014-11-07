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