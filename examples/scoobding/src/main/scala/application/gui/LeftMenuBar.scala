package gui

import scala.swing._

case class LeftMenuBar(components: Component*) extends MenuBar {
  contents += new FlowPanel(FlowPanel.Alignment.Left)(components:_*)
}
