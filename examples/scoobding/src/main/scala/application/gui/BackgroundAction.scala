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
package gui

import reactive._
import swing.SwingWorker

class BackgroundAction[T](action: =>T) {

  def inBackground(implicit progress: EventSource[ActionInProgress] = new EventSource[ActionInProgress]()): EventStream[T] = {
    val result: EventSource[T] = new EventSource
    val worker = new SwingWorker {
      def act() {
        try {
          progress.fire(Started())
          result.fire(action)
        } finally { progress.fire(Finished()) }
      }
    }
    worker.start()
    result
  }
}

object BackgroundAction {
  implicit def actionToBackgroundAction[T](action: =>T) = new BackgroundAction(action)
}

sealed trait ActionInProgress
case class Started() extends ActionInProgress
case class Finished() extends ActionInProgress
