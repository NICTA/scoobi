package gui

import swing._
import event.SelectionChanged
import Orientation._
import reactive.{EventStream, EventStreamSourceProxy}
import java.awt.Component

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
