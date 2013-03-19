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

import swing.{ComboBox, Orientation, BoxPanel}
import Orientation._
import swing.event.SelectionChanged
import reactive._
import java.awt.Component

/**
 * This class is used to display 2 combox boxes where the elements in the second box depend on the selected element in the first.
 *
 * It is also an EventStream for (T, S) where T is the first selected element and S is the second element
 */
class DependentComboBox[T, S](main: Seq[T], dependent: T => Seq[S], orientation: Orientation.Value = Horizontal) extends BoxPanel(orientation)
   with EventStreamSourceProxy[(T, S)] {

  override def self: Component with EventStream[(T, S)] = this.asInstanceOf[Component with EventStream[(T, S)]]

  lazy val mainComboBox = new ComboBox(main)

  // the second combobox needs to be recreated because the ComboBox model is immutable
  var dependentComboBox = new ComboBox(dependent(main.head))

  /** updated contents on startup */
  updateContents()

  /**
   * add the 2 comboboxes and listen to them
   */
  private def updateContents() {
    contents.clear()
    contents += mainComboBox
    contents += dependentComboBox
    listenTo(mainComboBox.selection)
    listenTo(dependentComboBox.selection)
  }

  reactions += {
    case SelectionChanged(_) => {
      val selectedDependent = {
        val selectedDep = dependentComboBox.selection.item
        if (dependent(selectedMain).contains(selectedDep)) selectedDep
        else                                               dependent(selectedMain).head

      }
      source.fire((selectedMain, selectedDependent))

      /** recreate the dependent box */
      dependentComboBox = new ComboBox(dependent(selectedMain))
      dependentComboBox.selection.item = selectedDependent
      updateContents()
    }
  }

  private def selectedMain = mainComboBox.selection.item
  /**
   * Testing functions
   */
  def selectMain(t: T): this.type = {
    mainComboBox.selection.item = t
    this
  }

  def selectDependent(s: S) = {
    dependentComboBox.selection.item = s
    this
  }


}