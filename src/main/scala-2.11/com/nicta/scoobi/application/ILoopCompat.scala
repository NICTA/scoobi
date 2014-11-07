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
package com.nicta.scoobi.application

import scala.tools.nsc.interpreter.ILoop

// Without using addThunk we run into deadlock issues in 2.10
// There doesn't appear to be a common function between 2.10 and 2.11 that will preserve the same behaviour
trait ILoopCompat extends ILoop {

  def addThunk(f: => Unit): Unit =
  // Evaluating 'f' directly still works, but the scoobi> prompt is shown immediately after "press Enter"
    intp.initialize(f)
}
