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
package text

/**
 * This class helps with the nice display of text.
 *
 * If a text is longer than lineSize, it will break the text at the first space and wrap the rest.
 */
case class PrettyPrinter(lineSize: Option[Int] = None, separator: String = "\\s") {
  def print(text: String) = printLines(text)._2

  private
  def printLines(text: String) : (Int,  String) = {

    lineSize.map  { maxSize =>
      if (text.size < maxSize) (text.size, text)
      else
        text.split(separator).foldLeft((0, "")) { (res, cur) =>
          val (currentLineSize, result) = res
          if (result.isEmpty)                            (cur.size, cur)
          else if (currentLineSize + cur.size < maxSize) (currentLineSize + cur.size + 1, result+" "+cur)
          else if (cur.size > maxSize)                   {
            val (lastSize, printOnCommas) = PrettyPrinter(maxSize, "\\,").printLines(cur)
            (lastSize, result+"\n"+printOnCommas)
          }
          else                                           (cur.size, result+"\n"+cur)
        }
    }.getOrElse((text.size, text))
  }

  /**
   * @return a pretty printed line which can be displayed in a Swing tooltip with multilines
   */
  def asToolTip(text: String) = {
    "<html>"+print(text).replace("\n", "<br/>").replaceAll("\\s", "&nbsp;")+"</html>"
  }
}

object PrettyPrinter {
  def apply(lineSize: Int): PrettyPrinter = PrettyPrinter(Some(lineSize))
  def apply(lineSize: Int, separator: String): PrettyPrinter = PrettyPrinter(Some(lineSize), separator)
}