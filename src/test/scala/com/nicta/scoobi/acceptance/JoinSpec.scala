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
package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.mutable.NictaSimpleJobs
import JoinExample._
import impl.plan.comp.CompNodeData._

class JoinSpec extends NictaSimpleJobs {

  def employees(implicit sc: SC) =
    fromDelimitedInput("Rafferty,31",
      "Jones,33",
      "Steinberg,33",
      "Robinson,34",
      "Smith,34",
      "John,-1").collect { case name :: ALong(departmentId) :: _ => Employee(name, departmentId) }

  def departments(implicit sc: SC) =
    fromDelimitedInput("31,Sales",
      "33,Engineering",
      "34,Clerical",
      "35,Marketing").collect { case ALong(id) :: name :: _ => Department(id, name) }

  def employeesByDepartmentId(implicit sc: SC) = employees.by(_.departmentId)
  def departmentsById(implicit sc: SC) = departments.by(_.id)
  def employeesByDepartmentIdString(implicit sc: SC) = employees.by(_.departmentId).map { case (k, v) => (k.toString, v) }
  def departmentsByIdString(implicit sc: SC) = departments.by(_.id).map { case (k, v) => (k.toString, v) }

  "Inner join" >> { implicit sc: SC =>
    (employeesByDepartmentId join departmentsById).run.mkString === Seq(
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))").mkString
  }

  "Left join" >> { implicit sc: SC =>
    (employeesByDepartmentId joinLeft departmentsById).run.mkString === Seq(
      "(-1,(John,None))",
      "(31,(Rafferty,Some(Sales)))",
      "(33,(Jones,Some(Engineering)))",
      "(33,(Steinberg,Some(Engineering)))",
      "(34,(Robinson,Some(Clerical)))",
      "(34,(Smith,Some(Clerical)))").mkString
  }

  "Right join" >> { implicit sc: SC =>
    (employeesByDepartmentId joinRight departmentsById).run.mkString === Seq(
      "(31,(Some(Rafferty),Sales))",
      "(33,(Some(Jones),Engineering))",
      "(33,(Some(Steinberg),Engineering))",
      "(34,(Some(Robinson),Clerical))",
      "(34,(Some(Smith),Clerical))",
      "(35,(None,Marketing))").mkString
  }

  "Full outer join" >> { implicit sc: SC =>
    (employeesByDepartmentId joinFullOuter departmentsById).run.mkString === Seq(
      "(-1,(Some(John),None))",
      "(31,(Some(Rafferty),Some(Sales)))",
      "(33,(Some(Jones),Some(Engineering)))",
      "(33,(Some(Steinberg),Some(Engineering)))",
      "(34,(Some(Robinson),Some(Clerical)))",
      "(34,(Some(Smith),Some(Clerical)))",
      "(35,(None,Some(Marketing)))").mkString
  }

  "Block inner join" >> { implicit sc: SC =>
    (employeesByDepartmentId blockJoin departmentsById).run.map(_.toString).sorted.mkString === Seq(
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))").sorted.mkString
  }

  "Block inner join with 3 replications" >> { implicit sc: SC =>
    ((employeesByDepartmentId replicateBy 3) blockJoin departmentsById).run.map(_.toString).sorted.mkString === Seq(
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))").sorted.mkString
  }
}

object JoinExample {
  case class Employee(name: String, departmentId: Long) {
    override def toString = name
  }
  case class Department(id: Long, name: String) {
    override def toString = name
  }

  implicit val EmployeeFmt: WireFormat[Employee]     = mkCaseWireFormat(Employee, Employee.unapply _)
  implicit val DepartmentFmt: WireFormat[Department] = mkCaseWireFormat(Department, Department.unapply _)
}

