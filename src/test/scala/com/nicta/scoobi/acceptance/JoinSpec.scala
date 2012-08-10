package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import JoinExample._

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
}

object JoinExample {
  case class Employee(name: String, departmentId: Long) {
    override def toString = name
  }
  case class Department(id: Long, name: String) {
    override def toString = name
  }

  implicit val EmployeeFmt = mkCaseWireFormat(Employee, Employee.unapply _)
  implicit val DepartmentFmt = mkCaseWireFormat(Department, Department.unapply _)
}

