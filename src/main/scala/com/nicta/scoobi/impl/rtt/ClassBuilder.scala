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

import java.lang.reflect.{Modifier => RModifier, Array => RArray, Field => RField, Constructor => RConstructor}
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import javassist._
import impl.util.UniqueInt


/** A class for building a class at run-time. */
trait ClassBuilder {

  /** Implemented by sub-class. */
  def className: String

  /** Implemented by sub-class. */
  def extendClass: Class[_]

  /** Implemented by sub-classes. Used for adding methods, fields, etc to the class. */
  def build()

  val pool: ClassPool = {
    val p = new ClassPool
    p.appendClassPath(new LoaderClassPath(this.getClass.getClassLoader))
    p
  }

  /** The compile-time representation of the class being built. */
  val ctClass: CtClass = pool.makeClass(className, pool.get(extendClass.getName))

  /** Compile the definition and code for the class. */
  def toRuntimeClass: RuntimeClass = {
    build()
    val bytecodeStream = new ByteArrayOutputStream
    ctClass.toBytecode(new DataOutputStream(bytecodeStream))
    new RuntimeClass(className, ctClass.toClass, bytecodeStream.toByteArray)
  }

  /** Add typeclass support as a private static member field. */
  def addTypeClassModel(model: AnyRef, name: String) = {

    val tcClass = model.getClass
    val tcClassName = tcClass.getName
    val tcCtClass = pool.get(tcClassName)

    /* Typeclass model field member (static) */
    val writerField = new CtField(tcCtClass, name, ctClass)
    writerField.setModifiers(Modifier.PRIVATE | Modifier.STATIC)
    ctClass.addField(writerField, CtField.Initializer.byExpr("set" + name + "();"))

    /*
     * Setter for typeclass model field member. Reconstruct the model object
     * from above using reflection.
     */
    val body: String = makeTypeClassModel(model).toString

    val setterMethod = CtNewMethod.make(Modifier.PRIVATE | Modifier.STATIC,
                                          tcCtClass,
                                          "set" + name,
                                          Array(),
                                          Array(),
                                          body,
                                          ctClass)
    ctClass.addMethod(setterMethod)
  }

  /* A class that allows up to store an Object and its var name */
  private case class ObjectLookup() {

    private val data = new java.util.IdentityHashMap[AnyRef, String]()

    def add(o: AnyRef, varName: String) { data.put(o, varName) }
    def hasObject(o: AnyRef): Boolean = data.containsKey(o)
    def getObjectVarName(o: AnyRef): String = data.get(o)
  }

  /* Creates a java function (as a string) that returns a reconstructed version of model */
  private def makeTypeClassModel(model: AnyRef): StringBuilder = {

    val lookup = ObjectLookup()
    val sb = new StringBuilder()
    sb ++= "{ java.lang.reflect.Field modifiersField = java.lang.reflect.Field.class.getDeclaredField(\"modifiers\"); "
    sb ++= "modifiersField.setAccessible(true); "

    makeObjectCopier(sb, model, model.getClass, "topObject", lookup)

    sb ++= "return (" + model.getClass.getName + ") topObject; }"
  }

  /* Makes Java code that copies all the fields of 'obj', putting the results into pre-defined variable 'setVariable'  */
  private def makeFieldCopier(sb: StringBuilder, obj: AnyRef, parentObjectClassVariable: String, setVariable: String, lookup: ObjectLookup) {
    obj.getClass.getDeclaredFields.filter((r: RField) => ! RModifier.isStatic(r.getModifiers) ).foreach { field =>
      field.setAccessible(true)

      var subObjVariable = VarGenerator.next()
      var fieldVariable = VarGenerator.next()
      var fieldObj = field.get(obj)

      sb ++= "java.lang.reflect.Field "
      sb ++= fieldVariable
      sb ++= " = "
      sb ++= parentObjectClassVariable
      sb ++= ".getDeclaredField(\""
      sb ++= field.getName
      sb ++= "\");"

      sb ++= fieldVariable
      sb ++= ".setAccessible(true);"

      sb ++= "modifiersField.setInt("
      sb ++= fieldVariable
      sb ++= ", "
      sb ++= fieldVariable
      sb ++= ".getModifiers() & ~java.lang.reflect.Modifier.FINAL);";

      makeObjectCopier(sb, fieldObj, if (fieldObj == null || field.getType.isPrimitive) field.getType else fieldObj.getClass, subObjVariable, lookup)

      sb ++= fieldVariable
      sb ++= ".set("
      sb ++= setVariable
      sb ++= ", "
      sb ++= subObjVariable
      sb ++= ");"


      sb ++= "modifiersField.setInt("
      sb ++= fieldVariable
      sb ++= " , "
      sb ++= fieldVariable
      sb ++= (".getModifiers() | java.lang.reflect.Modifier.FINAL);")
    }
  }

  /* Makes Java code that copies fieldObj into a new variable, called 'objectVariableName' */
  private def makeObjectCopier(sb: StringBuilder, fieldObj: AnyRef, fieldType: Class[_], objectNameVariable: String, lookup: ObjectLookup) {

    if(lookup.hasObject(fieldObj)) {
      sb ++= "Object "
      sb ++= objectNameVariable
      sb ++= " = "

      sb ++= lookup.getObjectVarName(fieldObj)
      sb ++= ";"
    } else {
      lookup.add(fieldObj, objectNameVariable)

      if (fieldObj == null) {
        sb ++= "Object "
        sb ++= objectNameVariable
        sb ++= " = "
        sb ++= "null;"
      } else if (fieldType == classOf[Class[_]]) {
         var classNameVariable = VarGenerator.next()

         sb ++= "Object "
         sb ++= objectNameVariable
         sb ++= " = "
         sb ++= "Class.forName(\""
         sb ++= fieldObj.asInstanceOf[Class[_]].getName
         sb ++= "\");"

         sb ++= "Class "
         sb ++= classNameVariable
         sb ++= " = "
         sb ++= makeClassName(fieldType)
         sb ++= ";"
      } else if (fieldType.isPrimitive) {
        val fieldType = fieldObj.getClass

        sb ++= "Object "
        sb ++= objectNameVariable
        sb ++= " = "

        fieldType match {
          case CJBoolean    => sb ++= "new Boolean(" + fieldObj.toString + ");"
          case CJByte       => sb ++= "new Byte((byte) " +fieldObj.toString + ");"
          case CJCharacter  => sb ++= "new Character('" + fieldObj.toString + "');"
          case CJDouble     => sb ++= "new Double(" + fieldObj.toString + "d);"
          case CJFloat      => sb ++= "new Float(" + fieldObj + "f);"
          case CJInteger    => sb ++= "new Integer(" + fieldObj.toString + ");"
          case CJLong       => sb ++= "new Long(" + fieldObj.toString + "L);"
          case CJShort      => sb ++= "new Short(" + fieldObj.toString + ");"
          case _            => sys.error("Unknown Error. Fieldtype is:" + fieldType)
        }

      } else if (fieldType.isArray) {
        var len = RArray.getLength(fieldObj)

        sb ++= "Object "
        sb ++= objectNameVariable
        sb ++= " = "
        sb ++= "java.lang.reflect.Array.newInstance("
        sb ++= makeClassName(fieldType.getComponentType)
        sb ++= ", "
        sb ++= len.toString
        sb ++= ");"

        ((0 to (len-1)) foreach { (n: Int) =>
          var elementNameVariable = VarGenerator.next()
          val obj: Object = RArray.get(fieldObj, n)

          makeObjectCopier(sb, obj, fieldType.getComponentType, elementNameVariable, lookup)

          sb ++= "java.lang.reflect.Array.set("
          sb ++= objectNameVariable
          sb ++= ", "
          sb ++= n.toString
          sb ++= ", "
          sb ++= elementNameVariable
          sb ++= ");"
        })

      } else {
        var classNameVariable = VarGenerator.next()

        sb ++= "Class "
        sb ++= classNameVariable
        sb ++= " = "
        sb ++= makeClassName(fieldType)
        sb ++= ";"

        sb ++= "Object "
        sb ++= objectNameVariable
        sb ++= " = "
        sb ++= "sun.reflect.ReflectionFactory.getReflectionFactory().newConstructorForSerialization("
        sb ++= classNameVariable
        sb ++= ", Object.class.getDeclaredConstructor(new Class[0])).newInstance(null);"

        makeFieldCopier(sb, fieldObj, classNameVariable, objectNameVariable, lookup)
      }
    }
  }

  private def makeClassName(c: Class[_]): String = {
    c.getName match {
      case "byte" => "byte.class"
      case "short" => "short.class"
      case "int" => "int.class"
      case "long" => "long.class"
      case "float" => "float.class"
      case "double" => "double.class"
      case "boolean" => "boolean.class"
      case "char" => "char.class"
      case t => "Class.forName(\"" + t + "\")"
    }
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

  /* Java primitive wrapper classes. */
  private val CJBoolean   = classOf[java.lang.Boolean]
  private val CJCharacter = classOf[java.lang.Character]
  private val CJShort     = classOf[java.lang.Short]
  private val CJInteger   = classOf[java.lang.Integer]
  private val CJLong      = classOf[java.lang.Long]
  private val CJFloat     = classOf[java.lang.Float]
  private val CJDouble    = classOf[java.lang.Double]
  private val CJByte      = classOf[java.lang.Byte]


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
    if (c.isArray)
      classToJavaTypeString(c.getComponentType) + "[]"
    else
      c.getName
  }
}

object VarGenerator extends UniqueInt {
  def next(): String = "agVar" + get.toString
}
