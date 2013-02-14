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
package app

import org.jfree._
import chart._
import swing.{Orientation, BoxPanel}
import Orientation._
import reactive.{Var, Observing, Signal}
import measure._
import process.BarsChart

/**
 * A swing panel to display a Bars chart
 */
case class BarsPanel(title: Signal[String] = Var(""),
                            yLegend: Signal[String] = Var(""),
                            xLegend: Signal[String] = Var(""),
                            logarithmic: Boolean = true,
                            horizontal: Boolean = false,
                            measurement: Signal[Measurement] = Var(Measurement())) extends BoxPanel(Horizontal) with Observing {

  def setLinear = copy(logarithmic = false)
  def setLogaritmic = copy(logarithmic = true)
  def setHorizontal = copy(horizontal = true)
  def setVertical = copy(horizontal = false)

  val chart = new BarsChart(title, yLegend, xLegend, logarithmic, horizontal, measurement)(this).chart

  /**
   * the contents of this panel is a JFree ChartPanel
   */
  contents += new scala.swing.Component {
    override lazy val peer: javax.swing.JComponent = new ChartPanel(chart)
  }
}
