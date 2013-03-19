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

import swing.UIElement
import java.awt.{Cursor, Point, Toolkit}
import Images._
import reactive.{Observing, EventStream}

case class WaitCursor(parent: UIElement)(implicit actionInProgress: EventStream[ActionInProgress]) extends Observing {
  private lazy val waitCursor = Toolkit.getDefaultToolkit.createCustomCursor(getImage("clock-icon.png"), new Point(0,0), "cursor")

  actionInProgress.foreach { action => action match {
      case Started()  => parent.cursor = waitCursor
      case Finished() => parent.cursor = new Cursor(Cursor.DEFAULT_CURSOR)
    }
  }
}