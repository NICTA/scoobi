import org.specs2._
import runner.SpecificationsFinder._
import specification.Tags

class index extends Specification with Tags { def is =
  scoobiLinks("Scoobi specifications") ^ section("index")

  def scoobiLinks(t: String) = specifications().foldLeft(t.title) { (res, cur) => res ^ see(cur) }
}

