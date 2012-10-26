package com.nicta.scoobi
package impl
package control

private[scoobi]
trait Functions {
  implicit def logicalFunction[A](f: A => Boolean): LogicalFunction[A] = LogicalFunction(f)

  case class LogicalFunction[A](f: A => Boolean) {
    def ||(g: A => Boolean) = (a: A) => f(a) || g(a)
    def &&(g: A => Boolean) = (a: A) => f(a) && g(a)
    def unary_!             = (a: A) => !f(a)
  }
}

object Functions extends Functions
