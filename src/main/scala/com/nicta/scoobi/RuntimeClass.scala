/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** A class represnting a class that has been generated at run-time. */
class RuntimeClass(val name: String,
                   val clazz: Class[_],
                   val bytecode: Array[Byte])
