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
private[scoobi]
trait ImplicitParameters {

  trait ImplicitParam
  implicit lazy val implicitParam: ImplicitParam = new ImplicitParam {}

  trait ImplicitParam1
  implicit lazy val implicitParam1: ImplicitParam1 = new ImplicitParam1 {}

  trait ImplicitParam2
  implicit lazy val implicitParam2: ImplicitParam2 = new ImplicitParam2 {}

  trait ImplicitParam3
  implicit lazy val implicitParam3: ImplicitParam3 = new ImplicitParam3 {}

  trait ImplicitParam4
  implicit lazy val implicitParam4: ImplicitParam4 = new ImplicitParam4 {}

  trait ImplicitParam5
  implicit lazy val implicitParam5: ImplicitParam5 = new ImplicitParam5 {}

  trait ImplicitParam6
  implicit lazy val implicitParam6: ImplicitParam6 = new ImplicitParam6 {}

  trait ImplicitParam7
  implicit lazy val implicitParam7: ImplicitParam7 = new ImplicitParam7 {}

  trait ImplicitParam8
  implicit lazy val implicitParam8: ImplicitParam8 = new ImplicitParam8 {}

  trait ImplicitParam9
  implicit lazy val implicitParam9: ImplicitParam9 = new ImplicitParam9 {}

  trait ImplicitParam10
  implicit lazy val implicitParam10: ImplicitParam10 = new ImplicitParam10 {}
}

private[impl]
object ImplicitParameters extends ImplicitParameters
