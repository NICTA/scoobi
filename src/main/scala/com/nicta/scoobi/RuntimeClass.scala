/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.lang.reflect.{Modifier => RModifier, Array => _, _}
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import javassist._


/** A class represnting a class that has been generated at run-time. */
class RuntimeClass(val name: String,
                   val clazz: Class[_],
                   val bytecode: Array[Byte])


/** A class for building a class at run-time. */
class ClassBuilder(protected val newClassName: String, extendClass: Class[_]) {

  protected val pool: ClassPool = {
    val p = new ClassPool
    p.appendClassPath(new LoaderClassPath(this.getClass.getClassLoader))
    p
  }

  /** The compile-time representation of the class being built. */
  protected val ctClass: CtClass = pool.makeClass(newClassName, pool.get(extendClass.getName))


  /** Compile the definition and code for the class. */
  def toRuntimeClass(): RuntimeClass = {
    val bytecodeStream = new ByteArrayOutputStream
    ctClass.toBytecode(new DataOutputStream(bytecodeStream))
    new RuntimeClass(newClassName, ctClass.toClass, bytecodeStream.toByteArray)
  }

  /** Add typeclass support as a private static member field.
    *
    *     TODO - may need to handle primitive types better here.
    */
  protected def addTypeClassModel(model: AnyRef, name: String) = {

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
    var body: String = ""
    val numArgs = tcClass.getConstructors.head.getParameterTypes.length

    body += "{"
    if (tcClass.getDeclaredFields.exists(_.getName.contains("MODULE$"))) {
      body += "return (" + tcClassName + ")" + tcClassName + ".MODULE$;"
    } else {
      body += "Class objClass0 = Class.forName(\"" + tcClassName + "\");"
      body += "java.lang.reflect.Constructor[] constructors = objClass0.getConstructors();"
      if (numArgs == 0)
        body += "Object[] constructorArgs = null;"
      else
        body += "Object[] constructorArgs = {" + (0 until numArgs).map(_ => "null").mkString(",") + "};"
      body += "Object obj0 = constructors[0].newInstance(constructorArgs);"
      body += doFields(model, 0)
      body += "return (" + tcClassName + ")obj0;"
    }
    body += "}"

    val setterMethod = CtNewMethod.make(Modifier.PRIVATE | Modifier.STATIC,
                                          tcCtClass,
                                          "set" + name,
                                          Array(),
                                          Array(),
                                          body,
                                          ctClass)
    ctClass.addMethod(setterMethod)
  }

  /** */
  private def doFields(obj: AnyRef, n: Int): String = {
    var body = ""
    obj.getClass.getDeclaredFields.foreach { field =>
      field.setAccessible(true)
      val fieldObj = field.get(obj)
      val fieldClass = fieldObj.getClass
      val fieldClassName = fieldClass.getName

      body += "{"
      body += "java.lang.reflect.Field field = objClass" + n + ".getDeclaredField(\"" + field.getName + "\");"

      if (fieldClass.getDeclaredFields.exists(_.getName.contains("MODULE$"))) {
        body += "Object obj" + (n + 1) + " = " + fieldClassName + ".MODULE$;"
      } else {
        val numArgs = fieldClass.getConstructors.head.getParameterTypes.length

        body += "Class objClass" + (n + 1) + " = Class.forName(\"" + fieldClassName + "\");"
        body += "java.lang.reflect.Constructor[] constructors = objClass" + (n + 1) + ".getConstructors();"
        if (numArgs == 0)
          body += "Object[] constructorArgs = null;"
        else
          body += "Object[] constructorArgs = {" + (0 until numArgs).map(_ => "null").mkString(",") + "};"
        body += "Object obj" + (n + 1) + " = constructors[0].newInstance(constructorArgs);"
        body += doFields(fieldObj, n + 1)
      }

      body += "field.setAccessible(true);"
      body += "field.set(obj" + n + ", obj" + (n + 1) +");"
      body += "}"
    }
    body
  }
}

