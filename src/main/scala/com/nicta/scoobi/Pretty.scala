/**
  * Copyright: [2011] Sean Seefried
  *
  * Pretty printing functionality
  */

package com.nicta.scoobi

object Pretty {

  def indent(n: Int, s: String): String = {
    val lines = s.split("\n")
    lines.mkString("\n" + " " * n)
  }

  def indent(pre: String, s:String): String = {
    pre + indent(pre.length,s)
  }


}
