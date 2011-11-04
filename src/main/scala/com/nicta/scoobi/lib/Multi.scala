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
package com.nicta.scoobi.lib

import java.io._
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.WireFormat._


/** A support data structure for joins and co-groups. Essentially like the Either type
 *  but with 8 type parameters rather than 2. */
sealed abstract class Multi[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat]
case class Some1[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T1) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some2[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T2) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some3[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T3) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some4[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T4) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some5[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T5) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some6[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T6) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some7[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T7) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]
case class Some8[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat](x: T8) extends Multi[T1, T2, T3, T4, T5, T6, T7, T8]


object Multi {

  implicit def MultiFmt[T1 : WireFormat, T2 : WireFormat, T3 : WireFormat, T4 : WireFormat, T5 : WireFormat, T6 : WireFormat, T7 : WireFormat, T8 : WireFormat] =
    new WireFormat[Multi[T1, T2, T3, T4, T5, T6, T7, T8]] {

      def toWire(x: Multi[T1, T2, T3, T4, T5, T6, T7, T8], out: DataOutput) = x match {
        case Some1(x) => out.writeInt(1); implicitly[WireFormat[T1]].toWire(x, out)
        case Some2(x) => out.writeInt(2); implicitly[WireFormat[T2]].toWire(x, out)
        case Some3(x) => out.writeInt(3); implicitly[WireFormat[T3]].toWire(x, out)
        case Some4(x) => out.writeInt(4); implicitly[WireFormat[T4]].toWire(x, out)
        case Some5(x) => out.writeInt(5); implicitly[WireFormat[T5]].toWire(x, out)
        case Some6(x) => out.writeInt(6); implicitly[WireFormat[T6]].toWire(x, out)
        case Some7(x) => out.writeInt(7); implicitly[WireFormat[T7]].toWire(x, out)
        case Some8(x) => out.writeInt(8); implicitly[WireFormat[T8]].toWire(x, out)
      }

      def fromWire(in: DataInput): Multi[T1, T2, T3, T4, T5, T6, T7, T8] = {
        val i = in.readInt()
        i match {
          case 1 => { val x: T1 = implicitly[WireFormat[T1]].fromWire(in); Some1(x) }
          case 2 => { val x: T2 = implicitly[WireFormat[T2]].fromWire(in); Some2(x) }
          case 3 => { val x: T3 = implicitly[WireFormat[T3]].fromWire(in); Some3(x) }
          case 4 => { val x: T4 = implicitly[WireFormat[T4]].fromWire(in); Some4(x) }
          case 5 => { val x: T5 = implicitly[WireFormat[T5]].fromWire(in); Some5(x) }
          case 6 => { val x: T6 = implicitly[WireFormat[T6]].fromWire(in); Some6(x) }
          case 7 => { val x: T7 = implicitly[WireFormat[T7]].fromWire(in); Some7(x) }
          case 8 => { val x: T8 = implicitly[WireFormat[T8]].fromWire(in); Some8(x) }
          case _ => sys.error("Expecting value between 1 and 8, got " + i + ".")
        }
      }


      def show(x: Multi[T1, T2, T3, T4, T5, T6, T7, T8]) = x.toString
  }
}
