package com.nicta.scoobi.acceptance

import com.nicta.scoobi.testing.NictaHadoop
import com.nicta.scoobi.testing.mutable.SimpleJobs
import AverageAge._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text.TextInput.{ALong, AnInt}

class AverageAgeSpec extends NictaHadoop with SimpleJobs {
  "The average age of a list of persons can be computed as a distributed job" >> { implicit sc: SC =>
    val averages =
    fromInput("100,Ben,Lever,31",
              "101,Tom,Smith,45",
              "102,Michael,Robson,33",
              "103,Rami,Mukhatar,34",
              "104,Sean,Seefried,33",
              "105,Ben,Cool,27",
              "106,Tom,Selleck,66",
              "107,Michael,Jordan,48",
              "108,Rami,Yacoub,36",
              "109,Sean,Connery,81").run { list: DList[String] =>

      val persons = list.map { line: String =>
        val ALong(id) :: fN :: sN :: AnInt(age) :: _ = line.split(",").toList
        Person(id, sN, fN, age)
      }
      val nameAndAge = persons.map { p => (p.firstName, p.age) }
      val grouped = nameAndAge.groupByKey;
      grouped map { case (n, ages) => (n, average(ages)) }
    }

    averages.mkString(", ") must_== "(Ben,29), (Michael,40), (Rami,35), (Sean,57), (Tom,55)"
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
