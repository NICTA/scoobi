package report

import measure._

case class Report(name: String = "",
                  measured: String = "",
                  unit: String = "",
                  results: Measurement = Measurement(),
                  recordsNumber: Long = 0) {
  def startTime = results.startTime
  def endTime   = results.endTime
}
