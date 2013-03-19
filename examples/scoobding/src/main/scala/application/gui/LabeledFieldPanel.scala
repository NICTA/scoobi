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
import Orientation._
import reactive.{Observing, Signal}

class LabeledFieldPanel(label: String, text: Signal[String], editable: Boolean = false) extends BoxPanel(Horizontal) with Observing { outer =>
  protected lazy val textField = new TextField(text.now) { editable = outer.editable }

  contents += new Label(label+" ")
  contents += textField
  border = Swing.EtchedBorder(Swing.Lowered)

  text.change.foreach { t => textField.text = t }
}

