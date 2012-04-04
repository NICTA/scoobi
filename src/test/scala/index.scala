import org.specs2._
import runner.SpecificationsFinder._

class index extends Specification { def is =

  scoobiLinks("Scoobi specifications")

  def scoobiLinks(t: String) = specifications().foldLeft(t.title) { (res, cur) => res ^ see(cur) }
}

