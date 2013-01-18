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

