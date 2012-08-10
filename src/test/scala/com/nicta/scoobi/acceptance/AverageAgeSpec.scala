package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import AverageAge._
import io.text.TextInput.{ALong, AnInt}

class AverageAgeSpec extends NictaSimpleJobs {

  "The average age of a list of persons can be computed as a distributed job" >> { implicit sc: SC =>
    val input =
      fromInput("100,Ben,Lever,31",
                "101,Tom,Smith,45",
                "102,Michael,Robson,33",
                "103,Rami,Mukhatar,34",
                "104,Sean,Seefried,33",
                "105,Ben,Cool,27",
                "106,Tom,Selleck,66",
                "107,Michael,Jordan,48",
                "108,Rami,Yacoub,36",
                "109,Sean,Connery,81")

    val averages = {
      val persons = input.map(_.split(",").toList).
        collect { case ALong(id) :: fN :: sN :: AnInt(age) :: _ => Person(id, sN, fN, age) }

      val nameAndAge = persons.map { p => (p.firstName, p.age) }
      val grouped    = nameAndAge.groupByKey
      grouped map { case (n, ages) => (n, average(ages)) }
    }

    averages.run.mkString(", ") must_== "(Ben,29), (Michael,40), (Rami,35), (Sean,57), (Tom,55)"
  }
}

object AverageAge {
  case class Person(id: Long, secondName: String, firstName: String, age: Int)
  implicit val PersonFmt: WireFormat[Person] = mkCaseWireFormat(Person, Person.unapply _)

  def average[A](values: Iterable[A])(implicit ev: Numeric[A]) = {
    import ev._

    var value: Int = 0
    var count = 0

    for (i <- values) {
      value = value + toInt(i)
      count = count + 1
    }

    value / count
  }
}
