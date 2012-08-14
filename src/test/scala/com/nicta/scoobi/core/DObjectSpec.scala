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
package core

import testing.NictaHadoop
import Scoobi._

class DObjectSpec extends NictaHadoop {

  tag("issue 113")
  "it must be possible to take the minimum and the maximum of a list" >> { implicit sc: SC =>
    val r = DList(1, 2, 3, 4)
    persist(r.min, r.max) === (1, 4)
  }

}
