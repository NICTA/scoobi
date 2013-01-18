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

