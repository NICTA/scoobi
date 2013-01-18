package gui

import swing._
import javax.swing.KeyStroke
import java.awt.event.{ActionEvent, KeyEvent}
import Images._
import java.io.File
import reactive._
import swing.FileChooser.Result
import java.awt.Component

case class OpenFileMenuItem(start: String, label: String = "Open") extends MenuItem(label) with EventStreamSourceProxy[File] { outer =>
  override def self: Component with EventStream[File] = this.asInstanceOf[Component with EventStream[File]]

  val fileChooser = new FileChooser(new java.io.File(start))

  action = new Action(label) {
    icon = getIcon("folder-icon.png")
    mnemonic = KeyEvent.VK_O
    accelerator = Some(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.ALT_MASK))

    def apply() {
      fileChooser.showOpenDialog(outer) match {
        case Result.Approve => source.fire(fileChooser.selectedFile)
        case _              => ()
      }
    }
  }
}


