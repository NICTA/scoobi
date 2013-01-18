package gui

import swing._
import javax.swing.KeyStroke
import java.awt.event.{ActionEvent, KeyEvent}
import Images._
import java.io.File
import reactive._
import FileChooser._
import java.awt.Component
import reactive.EventStreamSourceProxy

case class OpenFilesMenuItem(start: String, label: String = "Open") extends MenuItem(label) with EventStreamSourceProxy[Seq[File]] { outer =>
  override def self: Component with EventStream[Seq[File]] = this.asInstanceOf[Component with EventStream[Seq[File]]]

  val fileChooser = new FileChooser(new java.io.File(start))
  fileChooser.peer.setMultiSelectionEnabled(true)

  /**
   * @return the selected files
   */
  def selectedFiles: Seq[File] = Option(fileChooser.selectedFiles).toSeq.flatten

  action = new Action(label) {
    icon = getIcon("folder-icon.png")
    mnemonic = KeyEvent.VK_O
    accelerator = Some(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.ALT_MASK))

    def apply() {
      fileChooser.showOpenDialog(outer) match {
        case Result.Approve => source.fire(selectedFiles)
        case _              => ()
      }
    }
  }
}


