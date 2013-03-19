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


import gui._
import swing._
import java.awt.Dimension
import reactive._
import report._
import Images._
import BackgroundAction._
import measure._
import time.DaytimeRange
import scalaz.Semigroup
import reactive.Trigger

/**
 * This is the entry point for the HealthCheck application. It defines:
 *
 *  * the main frame with a menu and a main panel
 *  * the actions which can be triggered from gui components
 *  * the events created or consumed by gui elements
 *
 * This application uses Functional Reactive Programming (http://en.wikipedia.org/wiki/Functional_reactive_programming) to orchestrate
 * the actions between all the components
 *
 * @see the reactive package and http://github.com/nafg/reactive
 *
 */
trait Histogram extends SimpleSwingApplication with Observing {

  /**
   * Main frame. It is defined as implicit so that any Background action invoked with 'inBackground' can set a wait cursor on it
   */
   implicit lazy val top = new MainFrame {
     title = "Histogram"
     size = new Dimension(1500, 1300)
     preferredSize = size
     iconImage = getImage("stats-icon.png")

     contents = PositionedBorderPanel(center = reportPanel.mainPanel)
  }

  /**
   * GUI components
   */
  /** CURSOR */
  val actionWaitCursor: WaitCursor = WaitCursor(top)
  /** REPORT */
  lazy val reportPanel: ReportPanel = new ReportPanel(reportReady.hold(Report()))
  /** STATUS */
  lazy val statusPanel: StatusPanel = new StatusPanel
  /** events */
  lazy val start = new Trigger {}
  lazy val reportReady: EventStream[Report] = start flatMap { _ => getReport.inBackground }
  /** this event source is implicit and is used by both the wait cursor as a target and by the 'background' actions for publishing */
  implicit lazy val actionProgress = new EventSource[ActionInProgress]

  /** @return a report to display */
  def getReport: Report

  start.trigger
}

object Histogram {
  implicit def SeqToHistogram[K : Semigroup, V : Numeric](seq: Seq[(K, V)]): ToHistogram[K, V] = new ToHistogram(seq)
  implicit def MapToHistogram[K : Semigroup, V : Numeric](map: Map[K, V]): ToHistogram[K, V] = SeqToHistogram(map.toSeq)
  class ToHistogram[K : Semigroup, V : Numeric](seq: Seq[(K, V)]) {
    val toLong = implicitly[Numeric[V]].toLong(_)

    def toHistogram(xLabel: String, yLabel: String) = new Histogram {
      def getReport = Report("Histogram", xLabel+"/"+yLabel, "", measurement, toLong((Map(seq:_*).values.sum)))
      def measurement = Measurement("Tweet", DaytimeRange.EMPTY, measures)
      def measures = seq.map { case (k, v) => Measure(yLabel, k, toLong(v)).copy(fullName = xLabel+" "+k) }
    }.main(Array())

  }
}