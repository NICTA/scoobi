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

import swing._
import event.SelectionChanged
import Orientation._
import reactive.{EventStream}
import java.awt.Component
import reactive.EventStreamSourceProxy

class LabeledComboBoxPanel[A](label: String, values: Seq[A]) extends BoxPanel(Horizontal) with EventStreamSourceProxy[String] {
  override def self: Component with EventStream[String] = this.asInstanceOf[Component with EventStream[String]]

  protected lazy val combo = new ComboBox(values)

  contents += new Label(label+" ")
  contents += combo
  border = Swing.EtchedBorder(Swing.Lowered)

  combo.reactions += {
    case SelectionChanged(c) => source.fire(combo.selection.item.toString)
  }
}
