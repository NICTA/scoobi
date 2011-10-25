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
package com.nicta.scoobi.io

import com.nicta.scoobi.impl.plan.AST


/** A data store represents data that is exterrnal to an MSCRs. As a consequence it is
  * external to a Hadoop job which means it must be perisisted somewhere, at least
  * temporarily, between jobs. There are three kinds: Inputs, Outputs and
  * Bridges. */
abstract class DataStore(val node: AST.Node[_]) {

  /* The name to be given to the type of this connector. Two different types can
   * not share the same name. */
  val typeName: String = "V" + node.id
}
