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
package collection

import Seqs._
import testing.mutable.UnitSpecification

class SeqsSpec extends UnitSpecification {

  "A sequence can be splitted into several smaller ones" >> {
    split(Seq(), 3, Splitted)           === Seq()
    split(Seq(1), 3, Splitted)          === Seq(Splitted(0, 1, Seq(1)))
    split(Seq(1, 2), 3, Splitted)       === Seq(Splitted(0, 2, Seq(1, 2)))
    split(Seq(1, 2, 3), 3, Splitted)    === Seq(Splitted(0, 3, Seq(1, 2, 3)))
    split(Seq(1, 2, 3, 4), 3, Splitted) === Seq(Splitted(0, 3, Seq(1, 2, 3, 4)), Splitted(3, 1, Seq(1, 2, 3, 4)))
  }

  case class Splitted(offset: Int, length: Int, seq: Seq[Int])
}

