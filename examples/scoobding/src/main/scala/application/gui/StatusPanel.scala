package gui

import java.awt.{Font, Color}
import java.io._
import swing._
import javax.swing.SwingUtilities

class StatusPanel extends TextArea { outer =>
  foreground = Color.blue
  font = new Font("Courier", Font.PLAIN, 11);
  rows = 5

  redirectSystemStreams()

  private def updateTextArea(text: String) {
    SwingUtilities.invokeLater(new Runnable() {
      def  run() {
        outer.append(text)
      }})
  }

  private def redirectSystemStreams() {
    val systemOut = System.out
    val out = new OutputStream {
      override def write(b: Int) {
        updateTextArea(String.valueOf(b))
        systemOut.println(String.valueOf(b))
      }

      override def write(b: Array[Byte], off: Int, len: Int) {
        updateTextArea(new String(b, off, len))
        systemOut.println(new String(b, off, len))
      }

      override def write(b: Array[Byte]) {
         write(b, 0, b.length)
      }
    }
    def printOut = new PrintStream(out, true)
    System.setOut(printOut)
    System.setErr(printOut)
    scala.Console.setOut(printOut)
    scala.Console.setErr(printOut)
  }

}