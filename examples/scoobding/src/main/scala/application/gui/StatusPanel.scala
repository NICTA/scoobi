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