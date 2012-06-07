import org.specs2._
import runner.SpecificationsFinder._
import specification.Tags

class AllSpecificationsIndex extends Specification with Tags { def is =

  "index".title.urlIs("index.html")    ^
  scoobiLinks("Scoobi specifications") ^ section("index")

  def scoobiLinks(t: String) = specifications().foldLeft(t.title) { (res, cur) => res ^ see(cur) }
}

