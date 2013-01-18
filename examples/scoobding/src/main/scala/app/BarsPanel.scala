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
