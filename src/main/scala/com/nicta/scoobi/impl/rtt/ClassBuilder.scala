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
package com.nicta.scoobi
package impl
package rtt

import java.io._
import javassist._

import impl.util.{Serialiser, DistCache, UniqueInt}
import core.{Grouping, WireFormat}
import com.thoughtworks.xstream.io.xml.StaxDriver
import com.thoughtworks.xstream.XStream

/** A class for building a class at run-time. */
trait ClassBuilder {

  /** Implemented by sub-class. */
  def className: String

  /** Implemented by sub-class. */
  def extendClass: Class[_]

  /** Implemented by sub-classes. Used for adding methods, fields, etc to the class. */
  def build()

  private val pool: ClassPool = {
    val pool = new ClassPool
    pool.appendClassPath(new LoaderClassPath(this.getClass.getClassLoader))
    pool
  }

  private def getType(typeName: String) = {
    typeName match {
      case "int"     => CtClass.intType
      case "void"    => CtClass.voidType
      case "boolean" => CtClass.booleanType
      case "byte"    => CtClass.byteType
      case "short"   => CtClass.shortType
      case "double"  => CtClass.doubleType
      case "float"   => CtClass.floatType
      case "long"    => CtClass.longType
      case other     => pool.get(other)
    }
  }

  /** The compile-time representation of the class being built. */
  lazy val ctClass: CtClass = pool.makeClass(className, pool.get(extendClass.getName))
  /** The compile-time representation of the class once built. */
  lazy val builtCtClass = { build(); ctClass }
  /** The class bytecode */
  lazy val classBytecode = {
    val bytecodeStream = new ByteArrayOutputStream
    builtCtClass.toBytecode(new DataOutputStream(bytecodeStream))
    bytecodeStream.toByteArray
  }

  private lazy val code = new StringBuilder

  override lazy val toString = {
    builtCtClass
    Seq("class "+className+" extends "+extendClass+" {",
          indent(code.toString),
        "}").mkString("\n")
  }
  private def indent(string: String) = {
    string.split("\n").map(line => if (line.nonEmpty) ("  "+line) else line).mkString("\n")
  }

  /** Compile the definition and code for the class. */
  def toRuntimeClass: RuntimeClass = {
    new RuntimeClass(className, builtCtClass.toClass, classBytecode)
  }

  def toClass: Class[_] = builtCtClass.toClass()

  protected def addStaticField(className: String, fieldName: String, initialiser: String) = {
    code.append("static ")
    val field = addPrivateField(className, fieldName, initialiser)
    field.setModifiers(Modifier.PRIVATE | Modifier.STATIC)
    field
  }

  protected def addPrivateField(className: String, name: String, initialiser: String = "") = {
    code.append("private ")
    val field = addField(className, name, initialiser)
    field.setModifiers(Modifier.PRIVATE)
    field
  }

  protected def addField(className: String, name: String, initialiser: String = "") = {
    code.append(className+" "+name+" =\n"+indent(initialiser)+"\n\n")

    val field = new CtField(pool.get(className), name, ctClass)
    if (initialiser.isEmpty) ctClass.addField(field)
    else                     ctClass.addField(field, CtField.Initializer.byExpr(initialiser))
    field
  }

  protected def addStaticMethod(className: String, methodName: String, body: =>String) = {
    code.append("static ")
    val method = addMethod(className, methodName, Array(), body)
    method.setModifiers(Modifier.PRIVATE | Modifier.STATIC)
    method
  }

  protected def addMethod(returnType: String, methodName: String, parameters: Array[String] = Array(), body: =>String) = {
    code.append(returnType+" "+methodName+parameters.zipWithIndex.map { case (p, i) => "$"+(i+1)+": "+p }.mkString(" (", ", ", ") {\n"))
    code.append(indent(body))
    code.append("\n}\n\n")

    val method = CtNewMethod.make(getType(returnType), methodName, parameters.map(getType), Array(), body, ctClass)
    ctClass.addMethod(method)
    method
  }

  /** Add typeclass support as a private static member field. */
  def addWireFormatField(wireFormat: WireFormat[_], name: String) {
    addInstantiatedField(wireFormat, name)
  }

  def addGroupingField(grouping: Grouping[_], name: String) {
    addInstantiatedField(grouping, name)
  }

  private def addInstantiatedField(obj: AnyRef, name: String) {
    val serialized = Serialiser.toByteArray(obj)
    val instance = "com.nicta.scoobi.impl.util.Serialiser$.MODULE$.fromByteArray(new byte[]{"+serialized.toSeq.mkString(",")+"});"
    addStaticField(obj.getClass.getName, name, initialiser = "("+obj.getClass.getName+") "+instance)
  }

  /* A class that allows up to store an Object */
  private case class ObjectLookup() {

    private val data = new java.util.IdentityHashMap[AnyRef, String]()

    def add(o: AnyRef, varName: String) { data.put(o, varName) }
    def hasObject(o: AnyRef): Boolean = data.containsKey(o)
    def getObjectVarName(o: AnyRef): String = data.get(o)
  }

  /* Scala value types. */
  private val CBoolean  = classOf[Boolean]
  private val CChar     = classOf[Char]
  private val CShort    = classOf[Short]
  private val CInt      = classOf[Int]
  private val CLong     = classOf[Long]
  private val CFloat    = classOf[Float]
  private val CDouble   = classOf[Double]
  private val CByte     = classOf[Byte]

  /** Generate code string for getter code handling primitive types. */
  protected def toObject(objCode: String, m: Manifest[_]): String = m.erasure match {
    case CBoolean => "((Boolean)("    + objCode + ")).booleanValue()"
    case CChar    => "((Character)("  + objCode + ")).charValue()"
    case CShort   => "((Short)("      + objCode + ")).shortValue()"
    case CInt     => "((Integer)("    + objCode + ")).intValue()"
    case CLong    => "((Long)("       + objCode + ")).longValue()"
    case CFloat   => "((Float)("      + objCode + ")).floatValue()"
    case CDouble  => "((Double)("     + objCode + ")).doubleValue()"
    case CByte    => "((Byte)("       + objCode + ")).byteValue()"
    case _        => "("              + objCode + ")"
  }

  /** Generate code string for setter code handling primitive types. */
  protected def fromObject(valCode: String, m: Manifest[_]): String = m.erasure match {
    case CBoolean => "new Boolean("   + valCode + ")"
    case CChar    => "new Character(" + valCode + ")"
    case CShort   => "new Short("     + valCode + ")"
    case CInt     => "new Integer("   + valCode + ")"
    case CLong    => "new Long("      + valCode + ")"
    case CFloat   => "new Float("     + valCode + ")"
    case CDouble  => "new Double("    + valCode + ")"
    case CByte    => "new Byte("      + valCode + ")"
    case _        =>                    valCode
  }

  /** Generate a Java type string from a Class object. */
  protected def classToJavaTypeString(c: Class[_]): String = {
    if (c.isArray) classToJavaTypeString(c.getComponentType) + "[]"
    else           c.getName
  }
}

object VarGenerator extends UniqueInt {
  def next(): String = "agVar" + get.toString
}
