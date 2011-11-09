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
import com.nicta.scoobi.impl.util.UniqueInt
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

  /* A class that allows up to store an Object */
  private case class ObjectLookup() {

    private val data = new java.util.HashMap[ObjectWrapper, String]()

    case class ObjectWrapper(data: AnyRef) {
       override def equals(other: Any): Boolean = {

         if (data.isInstanceOf[AnyRef] && other.isInstanceOf[ObjectWrapper])
           data.asInstanceOf[AnyRef] eq other.asInstanceOf[ObjectWrapper].data
         else
           false
       }
       override def hashCode(): Int ={
         1 // sigh... this is really dumb,
         // but scala case classes seem to like overriding
         // hasCode with something that's broken
         // So now our search is O(n) instead of O(~log n) =/
         // TODO: investigate if reflection can be used
         // to call Object::hashCode without dynamic dispatch
       }
    }

    def add(o: AnyRef, varName: String) {
      data.put(ObjectWrapper(o), varName)
    }

    def hasObject(o: AnyRef): Boolean = {
      data.containsKey(ObjectWrapper(o))
    }

    def getObjectVarName(o: AnyRef): String = data.get(ObjectWrapper(o))

  }

  /* Creates a java function (as a string) that returns a reconstructed version of model */
  private def makeTypeClassModel(model: AnyRef): StringBuilder = {

   val lookup = ObjectLookup()
    val sb = new StringBuilder()
    sb.append("{ java.lang.reflect.Field modifiersField = java.lang.reflect.Field.class.getDeclaredField(\"modifiers\"); ")
    sb.append("modifiersField.setAccessible(true); ")

    makeObjectCopier(sb, model, model.getClass, "topObject", lookup)

    sb.append("return (" + model.getClass.getName + ") topObject; }")
  }

  /* Makes Java code that copies all the fields of 'obj', putting the results into pre-defined variable 'setVariable'  */
  private def makeFieldCopier(sb: StringBuilder, obj: AnyRef, parentObjectClassVariable: String, setVariable: String, lookup: ObjectLookup) {
    obj.getClass.getDeclaredFields.filter((r: RField) => ! RModifier.isStatic(r.getModifiers) ).foreach { field =>
      field.setAccessible(true)

      var subObjVariable = VarGenerator.next()
      var fieldVariable = VarGenerator.next()
      var fieldObj = field.get(obj)

      sb.append("java.lang.reflect.Field ")
      sb.append(fieldVariable)
      sb.append(" = ")
      sb.append(parentObjectClassVariable)
      sb.append(".getDeclaredField(\"")
      sb.append(field.getName)
      sb.append("\");")

      sb.append(fieldVariable)
      sb.append(".setAccessible(true);")

      sb.append("modifiersField.setInt(")
      sb.append(fieldVariable)
      sb.append(", ")
      sb.append(fieldVariable)
      sb.append(".getModifiers() & ~java.lang.reflect.Modifier.FINAL);");

      makeObjectCopier(sb, fieldObj, if (fieldObj == null || field.getType.isPrimitive) field.getType else fieldObj.getClass, subObjVariable, lookup)

      sb.append(fieldVariable)
      sb.append(".set(")
      sb.append(setVariable)
      sb.append(", ")
      sb.append(subObjVariable)
      sb.append(");")


      sb.append("modifiersField.setInt(")
      sb.append(fieldVariable)
      sb.append(" , ")
      sb.append(fieldVariable)
      sb.append(".getModifiers() | java.lang.reflect.Modifier.FINAL);")
    }
  }

  /* Makes Java code that copies fieldObj into a new variable, called 'objectVariableName' */
  private def makeObjectCopier(sb: StringBuilder, fieldObj: AnyRef, fieldType: Class[_], objectNameVariable: String, lookup: ObjectLookup) {

    if(lookup.hasObject(fieldObj)) {
      sb.append("Object ")
      sb.append(objectNameVariable)
      sb.append(" = ")

      sb.append(lookup.getObjectVarName(fieldObj))
      sb.append(";")
    } else {
      lookup.add(fieldObj, objectNameVariable)

      if (fieldObj == null) {
        sb.append("Object ")
        sb.append(objectNameVariable)
        sb.append(" = ")
        sb.append("null;")
      } else if (fieldType == classOf[Class[_]]) {
         var classNameVariable = VarGenerator.next()

         sb.append("Object ")
         sb.append(objectNameVariable)
         sb.append(" = ")
         sb.append("Class.forName(\"")
         sb.append(fieldObj.asInstanceOf[Class[_]].getName)
         sb.append("\");")

         sb.append("Class ")
         sb.append(classNameVariable)
         sb.append(" = ")
         sb.append(makeClassName(fieldType))
         sb.append(";")
      } else if (fieldType.isPrimitive) {
        val fieldType = fieldObj.getClass

        sb.append("Object ")
        sb.append(objectNameVariable)
        sb.append(" = ")

        fieldType match {
          case CJBoolean    => sb.append("new Boolean(" + fieldObj.toString + ");")
          case CJByte       => sb.append("new Byte(" +fieldObj.toString + ");")
          case CJCharacter  => sb.append("new Character('" + fieldObj.toString + "');")
          case CJDouble     => sb.append("new Double(" + fieldObj.toString + "d);")
          case CJFloat      => sb.append("new Float(" + fieldObj + "f);")
          case CJInteger    => sb.append("new Integer(" + fieldObj.toString + ");")
          case CJLong       => sb.append("new Long(" + fieldObj.toString + "L);")
          case CJShort      => sb.append("new Short(" + fieldObj.toString + ");")
          case _            => sys.error("Unknown Error. Fieldtype is:" + fieldType)
        }

      } else if (fieldType.isArray) {
        var len = RArray.getLength(fieldObj)

        sb.append("Object ")
        sb.append(objectNameVariable)
        sb.append(" = ")
        sb.append("java.lang.reflect.Array.newInstance(")
        sb.append(makeClassName(fieldType.getComponentType))
        sb.append(", ")
        sb.append(len)
        sb.append(");")

        ((0 to (len-1)) foreach { (n: Int) =>
          var elementNameVariable = VarGenerator.next()
          val obj: Object = RArray.get(fieldObj, n)

          makeObjectCopier(sb, obj, fieldType.getComponentType, elementNameVariable, lookup)

          sb.append("java.lang.reflect.Array.set(")
          sb.append(objectNameVariable)
          sb.append(", ")
          sb.append(n)
          sb.append(", ")
          sb.append(elementNameVariable)
          sb.append(");")
        })

      } else {
        var classNameVariable = VarGenerator.next()

        sb.append("Class ")
        sb.append(classNameVariable)
        sb.append(" = ")
        sb.append(makeClassName(fieldType))
        sb.append(";")

        sb.append("Object ")
        sb.append(objectNameVariable)
        sb.append(" = ")
        sb.append("sun.reflect.ReflectionFactory.getReflectionFactory().newConstructorForSerialization(")
        sb.append(classNameVariable)
        sb.append(", Object.class.getDeclaredConstructor(new Class[0])).newInstance(null);")

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

object VarGenerator extends UniqueInt {
  def next(): String = get.toString
}
