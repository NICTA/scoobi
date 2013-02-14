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
package process

import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.labels.CategoryToolTipGenerator
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import java.awt.{Graphics2D, Color}
import org.jfree.chart.axis._
import scala.Predef._
import measure._
import Measure._
import reactive.{Observing, Signal, Var}
import scalaz.Order
import text._
import org.jfree.ui.RectangleEdge
import java.awt.geom.Rectangle2D
import measure.Measurement
import org.jfree.text.TextBlock

/**
 * This class provide a JFree horizontal bars chart to display measurement
 *
 * The x scale can be made logarithmic and the chart can be adorned with a title and legends
 */
case class BarsChart(title: Signal[String] = Var(""),
                    yLegend: Signal[String] = Var(""),
                    xLegend: Signal[String] = Var(""),
                    logarithmic: Boolean = true,
                    horizontal: Boolean = false,
                    measurement: Signal[Measurement] = Var(Measurement()))(implicit obs: Observing) {

  def setLinear = copy(logarithmic = false)
  def setLogaritmic = copy(logarithmic = true)
  def setHorizontal = copy(horizontal = true)
  def setVertical = copy(horizontal = false)
  /**
   * a varying category dataset with the method names in the Y axis and the measures in the X axis
   */
  lazy val dataset = Var(new DefaultCategoryDataset) map { d =>
    measurement.foreach { ms => // access the measurement from the Signal
      d.clear() // clear the category set from previous measures
      ms.measures.foreach { measure =>
        measure.quantities.foreach { case quantity =>
          d.addValue(quantity.value, quantity.name, MeasureKey(measure))
        }
      }
    }
    d
  }

  dataset.change.foreach { d =>
    chart.setTitle(title.now)
    chart.getCategoryPlot.getDomainAxis.setLabel(yLegend.now)
    chart.getCategoryPlot.getRangeAxis.setLabel(xLegend.now)
    chart.fireChartChanged()
  }

  lazy val chart = {
    val c = ChartFactory.createBarChart(title.now, yLegend.now, xLegend.now, dataset.now, if (horizontal) PlotOrientation.HORIZONTAL else PlotOrientation.VERTICAL, true, true, false)

    val plot = c.getCategoryPlot
    plot.setNoDataMessage("No data")
    plot.setRangeAxisLocation(AxisLocation.TOP_OR_LEFT)

    plot.getRenderer.setBaseToolTipGenerator(new CategoryToolTipGenerator() {
      def generateToolTip(dataset: CategoryDataset, row: Int, column: Int) = {
        PrettyPrinter(lineSize = 100).asToolTip(dataset.getColumnKey(column).asInstanceOf[MeasureKey].show(dataset.getValue(row, column).longValue()))
      }
    })

    c.setBackgroundPaint(new Color(255, 255, 255))

    val renderer = c.getCategoryPlot.getRenderer
    renderer.setSeriesPaint(0, new Color(128, 0, 0))
    renderer.setSeriesItemLabelsVisible(0, true)
    renderer.setSeriesItemLabelsVisible(1, true)

    if (logarithmic) {
      val axis = new LogarithmicAxis(xLegend.now)
      axis.setStrictValuesFlag(false)
      plot.setRangeAxis(axis)
    }
    plot.getRangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits)
    val domainAxis = new SparseCategoryAxis(yLegend.now)
    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)
    plot.setDomainAxis(domainAxis)
    c
  }

}
import scala.collection.JavaConverters._

case class SparseCategoryAxis(label: String, labeledTicks: Int = 20) extends CategoryAxis {
  override def refreshTicks(g2: Graphics2D, state: AxisState, dataArea: Rectangle2D, edge: RectangleEdge): java.util.List[_] = {
    val standardTicks = super.refreshTicks(g2, state, dataArea, edge)
    val tickEvery = standardTicks.size / labeledTicks
    if (standardTicks.isEmpty || tickEvery < 1) {
      standardTicks
    } else {
      val tick = standardTicks.get(0).asInstanceOf[CategoryTick]
      standardTicks.asScala.map(_.asInstanceOf[CategoryTick]).zipWithIndex.map { case (t, i) =>
        if (i % tickEvery == 0) new CategoryTick(tick.getCategory, t.getLabel, tick.getLabelAnchor, tick.getRotationAnchor, 45)
        else                    new CategoryTick(tick.getCategory, new TextBlock, tick.getLabelAnchor, tick.getRotationAnchor, 45)
      }.asJava
    }
  }
}

/**
 * This class encapsulate a Measure so that only the name is used for column key comparisons in the Category Dataset
 */
case class MeasureKey(measure: Measure[_]) extends Comparable[MeasureKey] {
  def show(value: Long) = measure.show(value)
  def key = measure.key

  def compareTo(o: MeasureKey) = (key, o.key) match {
    case (k: RangeKey, ok: RangeKey) => Order[RangeKey].order(k, ok).toInt
    case _                           => key.toString.compareTo(o.key.toString)
  }

  override def equals(o: Any) = {
    o match {
      case m @ MeasureKey(_) => key == m.key
      case _                 => false
    }
  }
  override def hashCode = key.hashCode
  override def toString = key.toString
}