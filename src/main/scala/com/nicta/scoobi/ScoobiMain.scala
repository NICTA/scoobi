package com.nicta.scoobi

import Scoobi._

object ScoobiMain extends ScoobiApp {
  lazy val threshold = 2
  val list = DList(1, 2, 3, 4)
  val result = list.filter(i => i > threshold).materialize
  persist(result.use)
  println(result.get)
}

