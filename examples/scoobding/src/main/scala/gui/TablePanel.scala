package gui

import swing._

case class TablePanel(table: Table) extends BoxPanel(Orientation.Vertical) {
  contents += table
  border = Swing.EtchedBorder(Swing.Lowered)
}
