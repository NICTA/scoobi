/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.examples

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat._
import com.nicta.scoobi.io.text.TextInput._
import com.nicta.scoobi.io.text.TextOutput._
import java.io._

/*
 * This example takes a list of names and ages in the form: <id>, <firstName>, <secondName>, <age>
 * then gets the average age for each first name.
 */

object AverageAge {

  def main(args: Array[String]) = withHadoopArgs(args) { _ =>

    if (!new File("output-dir").mkdir) {
      sys.error("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)")
    }

    val fileName = "output-dir/names.txt"

    // write some names to a file (so this example has no external requirements)
    generateNames(fileName)

    case class Person(val id: Long,
                      val secondName: String,
                      val firstName: String,
                      val age: Int)

    // With this implicit conversion, we let Scoobi know the apply and unapply function, which it uses
    // to construct and deconstruct Person objects. Now it can very efficiently serialize them (i.e. no overhead)
    implicit val PersonFmt = mkCaseWireFormat(Person, Person.unapply _)


    // Read in lines of the form: 234242, Bob, Smith, 31.
    val persons : DList[Person] = extractFromDelimitedTextFile(",", fileName) {
      case Long(id) :: fN :: sN :: Int(age) :: _ => Person(id, sN, fN, age)
    }

    // The only thing we're interested in, is the firstName and age
    val nameAndAge: DList[(String, Int)] = persons.map { p => (p.firstName, p.age) }

    // Let's group everyone with the same name together
    val grouped: DList[(String, Iterable[Int])] = nameAndAge groupByKey

    // And for every name, we will average all the avages
    val avgAgeForName: DList[(String, Int)] = grouped map { case (n, ages) => (n, average(ages)) }

    // Execute everything, and throw it into a directory
    DList.persist (toTextFile(avgAgeForName, "output-dir/avg-age"))
  }

  private def average[A](values: Iterable[A])(implicit ev: Numeric[A]) = {
    import ev._
    toInt(values.sum) / values.size
  }


  private def generateNames(filename: String) {
    val fstream = new FileWriter(filename)

    fstream write ("""100,Ben,Lever,31
101,Tom,Smith,45
102,Michael,Robson,33
103,Rami,Mukhatar,34
104,Sean,Seefried,33
105,Ben,Cool,27
106,Tom,Selleck,66
107,Michael,Jordan,48
108,Rami,Yacoub,36
109,Sean,Connery,81""")

    fstream close()
  }
}
