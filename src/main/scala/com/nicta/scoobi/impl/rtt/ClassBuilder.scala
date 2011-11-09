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
package com.nicta.scoobi.impl.rtt

import java.lang.reflect.{Modifier => RModifier, Array => RArray, Field => RField, Constructor => RConstructor}
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import javassist._


/** A class for building a class at run-time. */
trait ClassBuilder {

  /** Implemented by sub-class. */
  def className: String

  /** Implemented by sub-class. */
  def extendClass: Class[_]

  /** Implemented by sub-classes. Used for adding methods, fields, etc to the class. */
  def build: Unit

  val pool: ClassPool = {
    val p = new ClassPool
    p.appendClassPath(new LoaderClassPath(this.getClass.getClassLoader))
    p
  }

  /** The compile-time representation of the class being built. */
  val ctClass: CtClass = pool.makeClass(className, pool.get(extendClass.getName))

  /** Compile the definition and code for the class. */
  def toRuntimeClass(): RuntimeClass = {
    build
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
    var body: String = makeTypeClassModel(model)

    val setterMethod = CtNewMethod.make(Modifier.PRIVATE | Modifier.STATIC,
                                          tcCtClass,
                                          "set" + name,
                                          Array(),
                                          Array(),
                                          body,
                                          ctClass)
    ctClass.addMethod(setterMethod)
  }
  /* Creates a java function (as a string) that returns a reconstructed version of model */
  private def makeTypeClassModel(model: AnyRef): String = {
    "{" +
      "java.lang.reflect.Field modifiersField = java.lang.reflect.Field.class.getDeclaredField(\"modifiers\"); " +
      "modifiersField.setAccessible(true); " +
      makeObjectCopier(model, model.getClass, "topObject") +
      "return (" + model.getClass.getName + ") topObject;" +
    "}"
  }

  /* Makes Java code that copies all the fields of 'obj', putting the results into pre-defined variable 'setVariable'  */
  private def makeFieldCopier(obj: AnyRef, parentObjectClassVariable: String, setVariable: String): String = {
    obj.getClass.getDeclaredFields.filter((r: RField) => ! RModifier.isStatic(r.getModifiers) ).map { field =>
      field.setAccessible(true)

      var subObjVariable = VarGenerator.next()
      var fieldVariable = VarGenerator.next()
      var fieldObj = field.get(obj)

      "java.lang.reflect.Field " + fieldVariable + " = " + parentObjectClassVariable + ".getDeclaredField(\"" + field.getName + "\");" +
      fieldVariable + ".setAccessible(true);" +
      "modifiersField.setInt(" + fieldVariable + ", " + fieldVariable + ".getModifiers() & ~java.lang.reflect.Modifier.FINAL);" +
      makeObjectCopier(fieldObj, if (fieldObj == null || field.getType.isPrimitive) field.getType else fieldObj.getClass, subObjVariable) +
      fieldVariable + ".set(" + setVariable + ", " + subObjVariable + ");" +
      "modifiersField.setInt(" + fieldVariable + " , " + fieldVariable + ".getModifiers() | java.lang.reflect.Modifier.FINAL);"
    }.foldLeft("")(_ + _)
  }

  /* Makes Java code that copies fieldObj into a new variable, called 'objectVariableName' */
  private def makeObjectCopier(fieldObj: AnyRef, fieldType: Class[_], objectNameVariable: String): String = {

    if (fieldObj == null) {
      "Object " + objectNameVariable + " = null;"
    } else if (fieldType == classOf[Class[_]]) {
       var classNameVariable = VarGenerator.next()

      "Class " + classNameVariable + " = " + makeClassName(fieldType) + ";" +
      "Object " + objectNameVariable + " = Class.forName(\"" + fieldObj.asInstanceOf[Class[_]].getName + "\");"
    } else if (fieldType.isPrimitive) {
      val fieldType = fieldObj.getClass

      "Object " + objectNameVariable + " = " +
      (if (fieldType == classOf[java.lang.Boolean])
       "new Boolean(" + fieldObj.toString + ");"
      else if (fieldType == classOf[java.lang.Byte])
        "new Byte(" +fieldObj.toString + ");"
      else if (fieldType == classOf[java.lang.Character])
        "new Character('" + fieldObj.toString + "');"
      else if (fieldType == classOf[java.lang.Double])
        "new Double(" + fieldObj.toString + "d);"
      else if (fieldType == classOf[java.lang.Float])
        "new Float(" + fieldObj + "f);"
      else if (fieldType == classOf[java.lang.Integer])
        "new Integer(" + fieldObj.toString + ");"
      else if (fieldType == classOf[java.lang.Long])
        "new Long(" + fieldObj.toString + "L);"
      else if (fieldType == classOf[java.lang.Short])
        "new Short(" + fieldObj.toString + ");"
      else
        sys.error("Unknown Error. Fieldtype is:" + fieldType))
    } else if (fieldType.isArray) {
      var len = RArray.getLength(fieldObj)

      "Object " + objectNameVariable + " = java.lang.reflect.Array.newInstance(" + makeClassName(fieldType.getComponentType) + ", " + len + ");" +
      ((0 to (len-1)) map { (n: Int) =>
        var elementNameVariable = VarGenerator.next()
        val obj: Object = RArray.get(fieldObj, n)

        makeObjectCopier(obj, fieldType.getComponentType, elementNameVariable) +
        "java.lang.reflect.Array.set(" + objectNameVariable + ", " + n + ", " + elementNameVariable + ");"
      }).foldLeft("")(_ + _)

    } else {
      var classNameVariable = VarGenerator.next()

      "Class " + classNameVariable + " = " + makeClassName(fieldType) + ";" +
      "Object " + objectNameVariable + " = sun.reflect.ReflectionFactory.getReflectionFactory().newConstructorForSerialization(" + classNameVariable + ", Object.class.getDeclaredConstructor(new Class[0])).newInstance(null);" +
      makeFieldCopier(fieldObj, classNameVariable, objectNameVariable)
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
    case CFloat   => "((Floag)("      + objCode + ")).floatValue()"
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
}

object VarGenerator {
  private var count = 0
  def next(): String = {
    count += 1
    "autoGenVar" + count.toString
  }
}
