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
package control

/**
 * This trait provides utility methods for functions
 */
private[scoobi]
trait Functions {
  implicit def logicalFunction[A](f: A => Boolean): LogicalFunction[A] = LogicalFunction(f)

  case class LogicalFunction[A](f: A => Boolean) {
    def ||(g: A => Boolean) = (a: A) => f(a) || g(a)
    def &&(g: A => Boolean) = (a: A) => f(a) && g(a)
    def unary_!             = (a: A) => !f(a)
  }
}
private[scoobi]
object Functions extends Functions
