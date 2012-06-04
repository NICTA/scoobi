package com.nicta.scoobi.acceptance

import com.nicta.scoobi.Scoobi
import Scoobi._
import JoinExample._
import com.nicta.scoobi.testing.NictaSimpleJobs

class JoinSpec extends NictaSimpleJobs {

  def employees(implicit sc: SC) =
    fromDelimitedInput("Rafferty,31",
                       "Jones,33",
                       "Steinberg,33",
                       "Robinson,34",
                       "Smith,34",
                       "John,-1").collect { case name :: ALong(departmentId) :: _ => Employee(name, departmentId) }.lines

  def departments(implicit sc: SC) =
    fromDelimitedInput("31,Sales",
                       "33,Engineering",
                       "34,Clerical",
                       "35,Marketing").collect { case ALong(id) :: name :: _ => Department(id, name) }.lines

  def employeesByDepartmentId(implicit sc: SC) = employees.by(_.departmentId)
  def departmentsById(implicit sc: SC)         = departments.by(_.id)

  "Inner join" >> { implicit sc: SC =>
    run(employeesByDepartmentId join departmentsById) === Seq(
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))")
  }

  "Left join" >> { implicit sc: SC =>
    run(employeesByDepartmentId joinLeft(departmentsById,
                (departmentId: Long, employee: Employee) => Department(departmentId, "Unknown"))) === Seq(
      "(-1,(John,Unknown))",
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))")
  }

  "Right join" >> { implicit sc: SC =>
    run(employeesByDepartmentId joinRight(departmentsById,
                 (id: Long, department: Department) => Employee("Unknown", id))) === Seq(
      "(31,(Rafferty,Sales))",
      "(33,(Jones,Engineering))",
      "(33,(Steinberg,Engineering))",
      "(34,(Robinson,Clerical))",
      "(34,(Smith,Clerical))",
      "(35,(Unknown,Marketing))")
  }

}

object JoinExample {
  case class Employee(name: String, departmentId: Long) {
    override def toString = name
  }
  case class Department(id: Long, name: String) {
    override def toString = name
  }

  implicit val EmployeeFmt   = mkCaseWireFormat(Employee, Employee.unapply _)
  implicit val DepartmentFmt = mkCaseWireFormat(Department, Department.unapply _)
}

