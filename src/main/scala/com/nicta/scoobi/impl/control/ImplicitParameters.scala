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
 * This trait can be used to overcome some limitations with method overloading due to type erasure
 */
trait ImplicitParameters {
  implicit lazy val implicitParameter: ImplicitParameter = new ImplicitParameter {}
  implicit lazy val implicitParameter1: ImplicitParameter1 = new ImplicitParameter1 {}
  implicit lazy val implicitParameter2: ImplicitParameter2 = new ImplicitParameter2 {}
  implicit lazy val implicitParameter3: ImplicitParameter3 = new ImplicitParameter3 {}
  implicit lazy val implicitParameter4: ImplicitParameter4 = new ImplicitParameter4 {}
  implicit lazy val implicitParameter5: ImplicitParameter5 = new ImplicitParameter5 {}
  implicit lazy val implicitParameter6: ImplicitParameter6 = new ImplicitParameter6 {}
  implicit lazy val implicitParameter7: ImplicitParameter7 = new ImplicitParameter7 {}
  implicit lazy val implicitParameter8: ImplicitParameter8 = new ImplicitParameter8 {}
  implicit lazy val implicitParameter9: ImplicitParameter9 = new ImplicitParameter9 {}
  implicit lazy val implicitParameter10: ImplicitParameter10 = new ImplicitParameter10 {}
}
trait ImplicitParameter
trait ImplicitParameter1
trait ImplicitParameter2
trait ImplicitParameter3
trait ImplicitParameter4
trait ImplicitParameter5
trait ImplicitParameter6
trait ImplicitParameter7
trait ImplicitParameter8
trait ImplicitParameter9
trait ImplicitParameter10

object ImplicitParameters extends ImplicitParameters
