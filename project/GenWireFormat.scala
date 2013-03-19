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
import sbt._

/**
 * This contains code to auto-generate WireFormats for objects made up of
 * multiple types (case classes, tuples, etc.).  It's not placed in a
 * package so it can be loaded from the Scala interpreter (REPL), and it's
 * given a suffix .scala.interp so that sbt won't try to compile it.
 *
 * The main entry point in `gen_files`, but see WireFormat.scala for more
 * detailed instructions as to how to use the code.
 */
object GenWireFormat {
  def gen(dir: File) = {
     val place = dir / "scoobi" / "codegen" / "GeneratedWireFormats.scala"
     IO.write(place,
"""package com.nicta.scoobi.codegen
import com.nicta.scoobi.core.WireFormat
import java.io._

object GeneratedWireFormats extends GeneratedWireFormats

trait GeneratedWireFormats {
""" + gen_section_1(22) + "\n\n" + gen_section_2(22) + "\n}")

     Seq(place)
  }

  def gen_section_1(maxargs: Int): String =
    ((2 to maxargs).map(gen_mkCaseWireFormat _) mkString "\n") + "\n" +
    ((2 to maxargs).map(gen_mkAbstractWireFormat _) mkString "\n")

  def gen_section_2(maxargs: Int): String =
    ((2 to maxargs).map(gen_mkTupleFmt _) mkString "\n")

  def gen_mkCaseWireFormat(numargs: Int) = {
    def gen(fmt: String, join: String) =
      (1 to numargs).map(fmt format _) mkString join
    def gen2(fmt: String, join: String) =
      (1 to numargs).map(x => fmt format (x, x)) mkString join
    def gen3(fmt: String, join: String) =
      (1 to numargs).map(x => fmt format (x, x, x)) mkString join
    def gen_raw_args = gen("A%d", ", ")
    def gen_raw_args_lc = gen("a%d", ", ")
    def gen_typed_args = gen("A%d: WireFormat", ", ")
    def gen_wf_args = gen("implicitly[WireFormat[A%d]]", ", ")
    def gen_toWire = {
      def gen_get_args = gen2(
        "      implicitly[WireFormat[A%d]].toWire(v._%d, out)", "\n")
"""    override def toWire(obj: T, out: DataOutput) {
      val v: Product%d[%s] = unapply(obj).get
%s
    }
""" format (numargs, gen_raw_args, gen_get_args)
    }
    def gen_fromWire = {
      def gen_get_args = gen3(
        "      val a%d: A%d = implicitly[WireFormat[A%d]].fromWire(in)", "\n")
"""    override def fromWire(in: DataInput): T = {
%s
      apply(%s)
    }
""" format (gen_get_args, gen_raw_args_lc)
    }
    def gen_toString = """    override def toString = "Case%d("+Seq(%s).mkString(",")+")" """ format (numargs, gen_wf_args)

"""  class Case%dWireFormat[T, %s](val apply: (%s) => T, val unapply: T => Option[(%s)]) extends WireFormat[T] {
%s
%s
%s
  }
  def mkCaseWireFormat[T, %s](apply: (%s) => T, unapply: T => Option[(%s)]): WireFormat[T] = new Case%dWireFormat(apply, unapply)
""" format (numargs, gen_typed_args, gen_raw_args, gen_raw_args,
            gen_toWire, gen_fromWire, gen_toString,
            gen_typed_args, gen_raw_args, gen_raw_args, numargs)
  }

  def gen_mkAbstractWireFormat(numargs: Int) = {
    def chr(x: Int) = 'A' + x - 1
    def gen(fmt: String, join: String) =
      (1 to numargs).map(fmt format chr(_)) mkString join
    def gen2(fmt: String, join: String) =
      (1 to numargs).map(x => fmt format (chr(x), chr(x))) mkString join
    def gen3(fmt: String, join: String) =
      (1 to numargs).map(
        x => fmt format (chr(x), chr(x), chr(x))) mkString join
    def gen4(fmt: String, join: String) =
      (1 to numargs).map(
        x => fmt format (chr(x), chr(x), chr(x), chr(x))) mkString join
    def gen_typed_args = gen("%C <: TT : Manifest : WireFormat", ", ")
    def gen_call_types = gen("%C", ", ")
    def gen_wireformats = gen("implicitly[WireFormat[%C]]", ",")
    def gen_toString = """    override def toString = "AbstractWF%d("+Seq(%s).mkString(",")+")" """ format (numargs, gen_wireformats)
    def gen_toWire = {
      def gen_if_else = gen4(
      """if (clazz == implicitly[Manifest[%c]].erasure) {
        out.writeInt('%c')
        implicitly[WireFormat[%c]].toWire(obj.asInstanceOf[%c], out)
      }""", " else ")
"""    override def toWire(obj: TT, out: DataOutput) {
      val clazz: Class[_] = obj.getClass

      %s else
        sys.error("Error in toWire. Don't know about type: " + clazz.toString)
    }
""" format gen_if_else
    }
    def gen_fromWire = {
      def gen_cases = gen2(
"        case '%c' => implicitly[WireFormat[%c]].fromWire(in)", "\n")
"""    override def fromWire(in: DataInput): TT =
      in.readInt() match {
%s
        case  x  => sys.error("Error in fromWire, don't know what " + x + " is")
      }
""" format gen_cases
    }
"""  class Abstract%dWireFormat[TT, %s]() extends WireFormat[TT] {
%s
%s
%s  }

  def mkAbstractWireFormat[TT, %s]() = new Abstract%dWireFormat[TT, %s]()
""" format (numargs, gen_typed_args, gen_toWire, gen_fromWire, gen_toString,
       gen_typed_args, numargs, gen_call_types)
  }

  def gen_mkTupleFmt(numargs: Int) = {
    def gen(fmt: String, join: String) =
      (1 to numargs).map(fmt format _) mkString join
    def gen2(fmt: String, join: String) =
      (1 to numargs).map(x => fmt format (x, x)) mkString join
    def gen3(fmt: String, join: String) =
      (1 to numargs).map(x => fmt format (x, x, x)) mkString join
    def gen_raw_args = gen("T%d", ", ")
    def lc_chr(x: Int) = 'a' + x - 1
    def gen_raw_args_lc_chr =
      (1 to numargs).map("%c" format lc_chr(_)) mkString ", "
    def gen_TupleFmt_args = gen2(
      "wt%d: WireFormat[T%d]", ", ")
    def gen_ClassFmt_args = gen2(
      "val wt%d: WireFormat[T%d]", ", ")
    def gen_calling_args = gen2(
      "wt%d", ", ")
    def gen_toWire_guts = gen2(
      "        wt%d.toWire(x._%d, out)", "\n")
    def gen_fromWire_guts =
      (1 to numargs).map(x =>
"        val %c = wt%d.fromWire(in)" format (lc_chr(x), x)) mkString "\n"
"""  class Tuple%dWireFormat[%s](%s) extends WireFormat[(%s)]
  {
      def toWire(x: (%s), out: DataOutput) {
%s
      }
      def fromWire(in: DataInput): (%s) = {
%s
        (%s)
      }
      override def toString = "("+Seq(%s).mkString(",")+")"
  }
  implicit def Tuple%dFmt[%s](implicit %s): WireFormat[(%s)] = new Tuple%dWireFormat(%s)
""" format (numargs, gen_raw_args, gen_ClassFmt_args, gen_raw_args,
           gen_raw_args,
           gen_toWire_guts,
           gen_raw_args,
           gen_fromWire_guts,
           gen_raw_args_lc_chr,
           gen_calling_args,
           numargs, gen_raw_args, gen_TupleFmt_args, gen_raw_args, numargs, gen_calling_args)
  }
}
