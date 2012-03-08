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

import com.nicta.scoobi.Scoobi._
import java.io._

/*
 * This example will show you how to apply different types of joins using the sample
 * dataset from the WikiPedia page on SQL Joins (http://en.wikipedia.org/wiki/Join_(SQL)),
 * slighly adjusted to be more OO/Scala.
 */

object JoinExamples {
  def main(args: Array[String]) = withHadoopArgs(args) { _ =>

    if (!new File("output-dir").mkdir) {
      sys.error("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)")
    }

    val employeesFile = "output-dir/employees.txt"
    val departmentsFile = "output-dir/departments.txt"

    // write some names to a file (so this example has no external requirements)
    generateDataSet(employeesFile, departmentsFile)

    case class Employee(val name: String, val departmentId: Long)
    case class Department(val id: Long, val name: String)
    
    // With this implicit conversion, we let Scoobi know the apply and unapply function, which it uses
    // to construct and deconstruct Employee and Department objects. 
    // Now it can very efficiently serialize them (i.e. no overhead)
    implicit val EmployeeFmt = mkCaseWireFormat(Employee, Employee.unapply _)
    implicit val DepartmentFmt = mkCaseWireFormat(Department, Department.unapply _)

    // Read in lines of the form: Bob Smith, 31
    val employees : DList[Employee] = fromDelimitedTextFile(employeesFile, ",") {
      case name :: Long(departmentId) :: _ => Employee(name, departmentId)
    }
    
    // Read in lines of the form: 31, Finance
    val departments : DList[Department] = fromDelimitedTextFile(departmentsFile, ",") {
      case Long(id) :: name :: _ => Department(id, name)
    }

    val employeesByDepartmentId: DList[(Long, Employee)] = employees.by(_.departmentId)
    val departmentsById: DList[(Long, Department)] = departments.by(_.id)

    // Perform an inner (equi)join
    val inner: DList[(Long, (Employee, Department))] = join(employeesByDepartmentId, departmentsById)

    // Perform a left outer join and specify what to do when the left has an
    // entry without a matching entry on the right
    val left: DList[(Long, (Employee, Department))] =
      joinLeft(employeesByDepartmentId,
               departmentsById,
               (departmentId, employee) => Department(departmentId, "Unknown"))

    // Perform a right outer join and specify what to do when the right has an
    // entry without a matching entry on the left
    val right: DList[(Long, (Employee, Department))] =
      joinRight(employeesByDepartmentId, departmentsById, (id, department) => Employee("Unknown", id))

    // Execute everything, and throw it into a directory
    DList.persist(
      toTextFile(inner, "output-dir/inner"),
      toTextFile(left, "output-dir/left"),
      toTextFile(right, "output-dir/right")
    )
  }

  private def generateDataSet(employeesFile: String, departmentsFile: String) {
    val e = new FileWriter(employeesFile)
    val d = new FileWriter(departmentsFile)

    e.write("""Rafferty,31
Jones,33
Steinberg,33
Robinson,34
Smith,34
John,-1""")

    e.close()

    d.write ("""31,Sales
33,Engineering
34,Clerical
35,Marketing""")

    d.close()
  }
}
