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

import report._
import gui._
import reactive.{Var, Signal}
import swing.{BoxPanel, Orientation}

/**
 * display the queries selection panel + the results + the total number of records in the store
 */
case class ReportPanel(report: Signal[Report]) { outer =>

  var mainPanel = PositionedBorderPanel(center = new BarsPanel(report.map(_.name),
                                                               report.map(_.measured),
                                                               report.map(_.unit),
                                                               logarithmic = true,
                                                               horizontal = false,
                                                               report.map(_.results)),
                                         south  = TotalRecords(report))

}


case class TotalRecords(report: Signal[Report] = Var(Report())) extends BoxPanel(Orientation.Horizontal) {

  contents += new LabeledFieldPanel("Start time", report.map(_.startTime))
  contents += new LabeledFieldPanel("End time", report.map(_.endTime))
  contents += new LabeledFieldPanel("Total records", report.map(_.recordsNumber.toString))

}

