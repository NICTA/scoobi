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

import Functions._
import testing.mutable.UnitSpecification

class FunctionsSpec extends UnitSpecification {

  "functions can be or-ed with ||" >> {
    val f1: String => Boolean = (_:String).length < 3
    val f2: String => Boolean = (_:String).length < 5

    (f1 || f2)("abcdefg") must beFalse
    (f1 || f2)("abc")     must beTrue
    (f1 || f2)("abcd")    must beTrue
    (f2 || f1)("ab")      must beTrue
  }
  "functions can be and-ed with &&" >> {
    val f1: String => Boolean = (_:String).length < 3
    val f2: String => Boolean = (_:String).length < 5
    (f1 && f2)("abcdefg") must beFalse
    (f1 && f2)("abc")     must beFalse
    (f1 && f2)("abcd")    must beFalse
    (f2 && f1)("ab")      must beTrue
  }
  "functions can be negated with !" >> {
    val f1: String => Boolean = (_:String).length < 3

    (!f1)("abcdefg") must beTrue
    (!f1)("ab")      must beFalse
  }

}
