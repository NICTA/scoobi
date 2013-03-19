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
package reactive

object Signalx {
  implicit def toVar[T](v: =>T): Var[T] = Var(v)
  implicit def toSignalx[T](s: Signal[T]) = new Signalx[T](s)
}

case class Signalx[T](s: Signal[T]) extends Observing {
  def changeIf[S](ev: EventStream[S]) = {
    val changed = Var(s.now)
    ev.foreach { b =>
      changed.update(s.now)
    }
    changed.change | s.change
  }
}